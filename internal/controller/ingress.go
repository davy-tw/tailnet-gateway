package controller

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"

	"go.pkgs.davy.tw/tailnet-gateway/internal/config"
)

const annotationDefaultIngressClass = "ingressclass.kubernetes.io/is-default-class"

// IngressReconciler translates Ingresses of the configured IngressClass into
// a Gateway and HTTPRoutes.
type IngressReconciler struct {
	generator
	Recorder events.EventRecorder
}

// NewIngressReconciler returns a reconciler.
func NewIngressReconciler(c client.Client, scheme *runtime.Scheme, cfg *config.Config, rec events.EventRecorder) *IngressReconciler {
	return &IngressReconciler{generator: generator{classResolver: classResolver{Client: c, Config: cfg}, Scheme: scheme}, Recorder: rec}
}

func (r *IngressReconciler) warn(obj runtime.Object, reason, msg string, args ...any) {
	if r.Recorder != nil {
		r.Recorder.Eventf(obj, nil, corev1.EventTypeWarning, reason, "Translate", msg, args...)
	}
}

// annotationLegacyIngressClass is the deprecated way to pick a class. The
// API server does not default spec.ingressClassName for Ingresses that set
// it.
const annotationLegacyIngressClass = "kubernetes.io/ingress.class"

// managed reports whether the Ingress belongs to this controller: its
// class (named, or the cluster default when none is named) is the
// configured IngressClass, and that class names this controller. An
// Ingress without spec.ingressClassName that names a class through the
// legacy annotation belongs to that class, never to the default one.
func (r *IngressReconciler) managed(ctx context.Context, ing *networkingv1.Ingress) (bool, error) {
	named := ing.Spec.IngressClassName != nil
	if named && *ing.Spec.IngressClassName != r.Config.IngressClass {
		return false, nil
	}
	if legacy, ok := ing.Annotations[annotationLegacyIngressClass]; !named && ok {
		if legacy != r.Config.IngressClass {
			return false, nil
		}
		named = true
	}
	ic := &networkingv1.IngressClass{}
	if err := r.Get(ctx, types.NamespacedName{Name: r.Config.IngressClass}, ic); err != nil {
		return false, client.IgnoreNotFound(err)
	}
	if ic.Spec.Controller != config.IngressControllerName {
		return false, nil
	}
	return named || ic.Annotations[annotationDefaultIngressClass] == "true", nil
}

// Reconcile implements reconcile.Reconciler.
func (r *IngressReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	ing := &networkingv1.Ingress{}
	if err := r.Get(ctx, req.NamespacedName, ing); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	managed, err := r.managed(ctx, ing)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !managed || !ing.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, r.cleanup(ctx, ing, false)
	}
	className := r.Config.DefaultGatewayClass
	if v := ing.Annotations[config.AnnotationGatewayClass]; v != "" {
		className = v
	}
	gc, err := r.managedClass(ctx, className)
	if err != nil {
		return ctrl.Result{}, err
	}
	if gc == nil {
		r.warn(ing, "GatewayClassNotManaged", "GatewayClass %q is not managed by this controller; skipping", className)
		return ctrl.Result{}, r.cleanup(ctx, ing, true)
	}

	gw, routes := r.translate(ctx, ing, className)
	if err := r.applyGenerated(ctx, ing, gw); err != nil {
		var ce *conflictError
		if errors.As(err, &ce) {
			r.warn(ing, "NameConflict", "%s", ce.msg)
			return ctrl.Result{RequeueAfter: conflictRequeue}, nil
		}
		return ctrl.Result{}, err
	}
	conflicted := false
	keep := map[string]bool{}
	for _, rt := range routes {
		keep[rt.Name] = true
		if err := r.applyGenerated(ctx, ing, rt); err != nil {
			var ce *conflictError
			if errors.As(err, &ce) {
				r.warn(ing, "NameConflict", "%s", ce.msg)
				conflicted = true
				continue
			}
			return ctrl.Result{}, err
		}
	}
	if err := r.pruneGenerated(ctx, ing, config.SourceIngress, &gwv1.HTTPRouteList{}, keep); err != nil {
		return ctrl.Result{}, err
	}
	return requeueIf(conflicted), r.writeStatus(ctx, ing, r.gatewayHostnames(ctx, ing.Namespace, ing.Name))
}

// cleanup removes the generated objects; clearStatus also clears the
// Ingress status (only when this controller still owns the Ingress).
func (r *IngressReconciler) cleanup(ctx context.Context, ing *networkingv1.Ingress, clearStatus bool) error {
	if err := r.pruneGenerated(ctx, ing, config.SourceIngress, &gwv1.HTTPRouteList{}, nil); err != nil {
		return err
	}
	if err := r.pruneGenerated(ctx, ing, config.SourceIngress, &gwv1.GatewayList{}, nil); err != nil {
		return err
	}
	if clearStatus && len(ing.Status.LoadBalancer.Ingress) > 0 {
		return r.writeStatus(ctx, ing, nil)
	}
	return nil
}

func (r *IngressReconciler) writeStatus(ctx context.Context, ing *networkingv1.Ingress, hostnames []string) error {
	var lbs []networkingv1.IngressLoadBalancerIngress
	for _, h := range hostnames {
		lbs = append(lbs, networkingv1.IngressLoadBalancerIngress{Hostname: h})
	}
	if reflect.DeepEqual(lbs, ing.Status.LoadBalancer.Ingress) || len(lbs) == 0 && len(ing.Status.LoadBalancer.Ingress) == 0 {
		return nil
	}
	orig := ing.DeepCopy()
	ing.Status.LoadBalancer.Ingress = lbs
	return r.Status().Patch(ctx, ing, client.MergeFrom(orig))
}

// translate builds the Gateway and HTTPRoutes for an Ingress. Rules are
// grouped by host so that each host only gets its own paths.
func (r *IngressReconciler) translate(ctx context.Context, ing *networkingv1.Ingress, className string) (*gwv1.Gateway, []*gwv1.HTTPRoute) {
	gw := &gwv1.Gateway{
		ObjectMeta: sourceMeta(ing, "Ingress", "networking.k8s.io/v1", config.SourceIngress, ing.Name),
		Spec:       gwv1.GatewaySpec{GatewayClassName: gwv1.ObjectName(className)},
	}
	if len(ing.Spec.TLS) > 0 {
		gw.Spec.Listeners = []gwv1.Listener{{Name: "https", Protocol: gwv1.HTTPSProtocolType, Port: 443, TLS: tailscaleTLSOptions()}}
	} else {
		gw.Spec.Listeners = []gwv1.Listener{{Name: "http", Protocol: gwv1.HTTPProtocolType, Port: 80}}
	}

	type group struct {
		host  string
		rules []gwv1.HTTPRouteRule
		// broken groups have a path whose backend cannot be used. They
		// are not served at all: leaving the path out would send its
		// requests to a shorter prefix that belongs to another backend.
		broken bool
	}
	var groups []*group
	byHost := map[string]*group{}
	for _, rule := range ing.Spec.Rules {
		g := byHost[rule.Host]
		if g == nil {
			g = &group{host: rule.Host}
			byHost[rule.Host] = g
			groups = append(groups, g)
		}
		if rule.HTTP == nil {
			continue
		}
		for _, p := range rule.HTTP.Paths {
			be, ok := r.backendRef(ctx, ing, p.Backend)
			if !ok {
				g.broken = true
				continue
			}
			path := p.Path
			if path == "" {
				path = "/"
			}
			typ := gwv1.PathMatchPathPrefix
			if p.PathType != nil && *p.PathType == networkingv1.PathTypeExact {
				typ = gwv1.PathMatchExact
				r.warn(ing, "UnsupportedPathType", "path %q: Exact paths cannot be expressed by Tailscale Services and are reported as unsupported on the HTTPRoute", path)
			}
			g.rules = append(g.rules, gwv1.HTTPRouteRule{
				Matches:     []gwv1.HTTPRouteMatch{{Path: &gwv1.HTTPPathMatch{Type: ptr.To(typ), Value: ptr.To(path)}}},
				BackendRefs: []gwv1.HTTPBackendRef{{BackendRef: be}},
			})
		}
	}
	// The default backend takes every request that no rule matches: the
	// rest of each host's paths, and hosts without rules (the route without
	// hostnames, served under the Ingress's name).
	if ing.Spec.DefaultBackend != nil {
		if be, ok := r.backendRef(ctx, ing, *ing.Spec.DefaultBackend); ok {
			fallback := gwv1.HTTPRouteRule{
				Matches:     []gwv1.HTTPRouteMatch{{Path: &gwv1.HTTPPathMatch{Type: ptr.To(gwv1.PathMatchPathPrefix), Value: ptr.To("/")}}},
				BackendRefs: []gwv1.HTTPBackendRef{{BackendRef: be}},
			}
			if byHost[""] == nil {
				g := &group{}
				byHost[""] = g
				groups = append(groups, g)
			}
			for _, g := range groups {
				if !hasRootPrefix(g.rules) {
					g.rules = append(g.rules, fallback)
				}
			}
		}
	}
	var routes []*gwv1.HTTPRoute
	for i, g := range groups {
		if g.broken {
			host := g.host
			if host == "" {
				host = "(no host)"
			}
			r.warn(ing, "HostNotServed", "host %s is not served until all of its backends resolve", host)
			continue
		}
		if len(g.rules) == 0 {
			continue
		}
		name := ing.Name
		if len(groups) > 1 {
			name = fmt.Sprintf("%s-%d", ing.Name, i)
		}
		rt := &gwv1.HTTPRoute{
			ObjectMeta: sourceMeta(ing, "Ingress", "networking.k8s.io/v1", config.SourceIngress, name),
			Spec: gwv1.HTTPRouteSpec{
				CommonRouteSpec: gwv1.CommonRouteSpec{ParentRefs: []gwv1.ParentReference{{Name: gwv1.ObjectName(ing.Name)}}},
				Rules:           g.rules,
			},
		}
		if g.host != "" {
			rt.Spec.Hostnames = []gwv1.Hostname{gwv1.Hostname(g.host)}
		}
		routes = append(routes, rt)
	}
	return gw, routes
}

// hasRootPrefix reports whether a rule already matches every path.
func hasRootPrefix(rules []gwv1.HTTPRouteRule) bool {
	for _, rule := range rules {
		for _, m := range rule.Matches {
			if m.Path != nil && m.Path.Type != nil && *m.Path.Type == gwv1.PathMatchPathPrefix &&
				m.Path.Value != nil && *m.Path.Value == "/" {
				return true
			}
		}
	}
	return false
}

// backendRef converts an Ingress backend, resolving named ports through
// the Service.
func (r *IngressReconciler) backendRef(ctx context.Context, ing *networkingv1.Ingress, b networkingv1.IngressBackend) (gwv1.BackendRef, bool) {
	if b.Service == nil {
		r.warn(ing, "UnsupportedBackend", "only Service backends are supported")
		return gwv1.BackendRef{}, false
	}
	port := b.Service.Port.Number
	if port == 0 && b.Service.Port.Name != "" {
		svc := &corev1.Service{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: ing.Namespace, Name: b.Service.Name}, svc); err != nil {
			r.warn(ing, "BackendNotFound", "Service %q for named port %q: %v", b.Service.Name, b.Service.Port.Name, err)
			return gwv1.BackendRef{}, false
		}
		for _, sp := range svc.Spec.Ports {
			if sp.Name == b.Service.Port.Name {
				port = sp.Port
			}
		}
		if port == 0 {
			r.warn(ing, "BackendNotFound", "Service %q has no port named %q", b.Service.Name, b.Service.Port.Name)
			return gwv1.BackendRef{}, false
		}
	}
	if port == 0 {
		port = 80
	}
	return gwv1.BackendRef{BackendObjectReference: gwv1.BackendObjectReference{
		Name: gwv1.ObjectName(b.Service.Name), Port: ptr.To(gwv1.PortNumber(port)),
	}}, true
}

// ingressesForService enqueues Ingresses in the Service's namespace (named
// ports may now resolve).
func (r *IngressReconciler) ingressesForService(ctx context.Context, obj client.Object) []reconcile.Request {
	var list networkingv1.IngressList
	if err := r.List(ctx, &list, client.InNamespace(obj.GetNamespace())); err != nil {
		return nil
	}
	var out []reconcile.Request
	for _, ing := range list.Items {
		out = append(out, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: ing.Namespace, Name: ing.Name}})
	}
	return out
}

func (r *IngressReconciler) allIngresses(ctx context.Context, _ client.Object) []reconcile.Request {
	var list networkingv1.IngressList
	if err := r.List(ctx, &list); err != nil {
		return nil
	}
	var out []reconcile.Request
	for _, ing := range list.Items {
		out = append(out, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: ing.Namespace, Name: ing.Name}})
	}
	return out
}

// SetupWithManager registers the reconciler.
func (r *IngressReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.APIReader = mgr.GetAPIReader()
	return ctrl.NewControllerManagedBy(mgr).
		Named("ingress").
		For(&networkingv1.Ingress{}).
		Owns(&gwv1.Gateway{}).
		Owns(&gwv1.HTTPRoute{}).
		Watches(&networkingv1.IngressClass{}, handler.EnqueueRequestsFromMapFunc(r.allIngresses)).
		Watches(&gwv1.GatewayClass{}, handler.EnqueueRequestsFromMapFunc(r.allIngresses)).
		Watches(&corev1.Service{}, handler.EnqueueRequestsFromMapFunc(r.ingressesForService),
			builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Complete(r)
}
