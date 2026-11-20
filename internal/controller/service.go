package controller

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/davy-tw/tailnet-gateway/internal/config"
)

// ServiceReconciler translates LoadBalancer Services of the configured
// loadBalancerClass into a Gateway with HTTP and TCP routes.
type ServiceReconciler struct {
	generator
	Features Features
	Recorder events.EventRecorder
}

// NewServiceReconciler returns a reconciler.
func NewServiceReconciler(c client.Client, scheme *runtime.Scheme, cfg *config.Config, f Features, rec events.EventRecorder) *ServiceReconciler {
	return &ServiceReconciler{generator: generator{classResolver: classResolver{Client: c, Config: cfg}, Scheme: scheme}, Features: f, Recorder: rec}
}

func (r *ServiceReconciler) warn(obj runtime.Object, reason, msg string, args ...any) {
	if r.Recorder != nil {
		r.Recorder.Eventf(obj, nil, corev1.EventTypeWarning, reason, "Translate", msg, args...)
	}
}

func (r *ServiceReconciler) managed(svc *corev1.Service) bool {
	return svc.Spec.Type == corev1.ServiceTypeLoadBalancer && svc.Spec.LoadBalancerClass != nil &&
		*svc.Spec.LoadBalancerClass == r.Config.LoadBalancerClass
}

// Reconcile implements reconcile.Reconciler.
func (r *ServiceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	svc := &corev1.Service{}
	if err := r.Get(ctx, req.NamespacedName, svc); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !svc.DeletionTimestamp.IsZero() || !r.managed(svc) {
		if !controllerutil.ContainsFinalizer(svc, config.Finalizer) {
			return ctrl.Result{}, nil
		}
		if err := r.cleanup(ctx, svc); err != nil {
			return ctrl.Result{}, err
		}
		orig := svc.DeepCopy()
		if svc.DeletionTimestamp.IsZero() && len(svc.Status.LoadBalancer.Ingress) > 0 {
			svc.Status.LoadBalancer.Ingress = nil
			if err := r.Status().Patch(ctx, svc, client.MergeFrom(orig)); err != nil {
				return ctrl.Result{}, err
			}
			orig = svc.DeepCopy()
		}
		controllerutil.RemoveFinalizer(svc, config.Finalizer)
		return ctrl.Result{}, r.Patch(ctx, svc, client.MergeFrom(orig))
	}
	if !controllerutil.ContainsFinalizer(svc, config.Finalizer) {
		orig := svc.DeepCopy()
		controllerutil.AddFinalizer(svc, config.Finalizer)
		if err := r.Patch(ctx, svc, client.MergeFrom(orig)); err != nil {
			return ctrl.Result{}, err
		}
	}
	className := r.Config.DefaultGatewayClass
	if v := svc.Annotations[config.AnnotationGatewayClass]; v != "" {
		className = v
	}
	gc, err := r.managedClass(ctx, className)
	if err != nil {
		return ctrl.Result{}, err
	}
	if gc == nil {
		r.warn(svc, "GatewayClassNotManaged", "GatewayClass %q is not managed by this controller; skipping", className)
		return ctrl.Result{}, r.cleanup(ctx, svc)
	}

	plan := PlanService(svc)
	for _, w := range plan.Warnings {
		r.warn(svc, "UnsupportedPort", "%s", w)
	}
	if len(plan.TCP) > 0 && !r.Features.TCPRoute {
		r.warn(svc, "TCPRouteUnavailable", "the TCPRoute CRD is not installed: TCP ports are not exposed")
		plan.TCP = nil
	}
	if plan.HTTP == nil && len(plan.TCP) == 0 {
		r.warn(svc, "NoPorts", "no port can be exposed")
		return ctrl.Result{}, r.cleanup(ctx, svc)
	}
	gw, httpRoutes, tcpRoutes := r.objects(svc, className, plan)
	if err := r.applyGenerated(ctx, svc, gw); err != nil {
		var ce *conflictError
		if errors.As(err, &ce) {
			r.warn(svc, "NameConflict", "%s", ce.msg)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	keepHTTP, keepTCP := map[string]bool{}, map[string]bool{}
	for _, rt := range httpRoutes {
		keepHTTP[rt.Name] = true
		if err := r.applyGenerated(ctx, svc, rt); err != nil {
			return ctrl.Result{}, ignoreConflict(err, func(m string) { r.warn(svc, "NameConflict", "%s", m) })
		}
	}
	for _, rt := range tcpRoutes {
		keepTCP[rt.Name] = true
		if err := r.applyGenerated(ctx, svc, rt); err != nil {
			return ctrl.Result{}, ignoreConflict(err, func(m string) { r.warn(svc, "NameConflict", "%s", m) })
		}
	}
	if err := r.pruneGenerated(ctx, svc, config.SourceService, &gwv1.HTTPRouteList{}, keepHTTP); err != nil {
		return ctrl.Result{}, err
	}
	if r.Features.TCPRoute {
		if err := r.pruneGenerated(ctx, svc, config.SourceService, &gwv1.TCPRouteList{}, keepTCP); err != nil {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{}, r.writeStatus(ctx, svc, r.gatewayHostnames(ctx, svc.Namespace, svc.Name))
}

func ignoreConflict(err error, report func(string)) error {
	var ce *conflictError
	if errors.As(err, &ce) {
		report(ce.msg)
		return nil
	}
	return err
}

func (r *ServiceReconciler) cleanup(ctx context.Context, svc *corev1.Service) error {
	if err := r.pruneGenerated(ctx, svc, config.SourceService, &gwv1.HTTPRouteList{}, nil); err != nil {
		return err
	}
	if r.Features.TCPRoute {
		if err := r.pruneGenerated(ctx, svc, config.SourceService, &gwv1.TCPRouteList{}, nil); err != nil {
			return err
		}
	}
	return r.pruneGenerated(ctx, svc, config.SourceService, &gwv1.GatewayList{}, nil)
}

func (r *ServiceReconciler) writeStatus(ctx context.Context, svc *corev1.Service, hostnames []string) error {
	var lbs []corev1.LoadBalancerIngress
	for _, h := range hostnames {
		lbs = append(lbs, corev1.LoadBalancerIngress{Hostname: h})
	}
	if reflect.DeepEqual(lbs, svc.Status.LoadBalancer.Ingress) || len(lbs) == 0 && len(svc.Status.LoadBalancer.Ingress) == 0 {
		return nil
	}
	orig := svc.DeepCopy()
	svc.Status.LoadBalancer.Ingress = lbs
	return r.Status().Patch(ctx, svc, client.MergeFrom(orig))
}

// ServicePlan is how a LoadBalancer Service's ports are exposed.
type ServicePlan struct {
	// Label is the Tailscale Service name shared by all ports.
	Label string
	// HTTP is the port served over HTTP(S), if any.
	HTTP     *corev1.ServicePort
	HTTPS    bool
	TCP      []corev1.ServicePort
	Warnings []string
}

var (
	httpAppProtocols  = map[string]bool{"http": true, "kubernetes.io/h2c": true, "kubernetes.io/ws": true}
	httpsAppProtocols = map[string]bool{"https": true, "kubernetes.io/wss": true}
)

// PlanService decides which ports become the HTTP listener and which
// become TCP forwards.
func PlanService(svc *corev1.Service) ServicePlan {
	plan := ServicePlan{Label: svc.Name}
	hostname := svc.Annotations[config.AnnotationHostname]
	if hostname != "" {
		plan.Label = hostname
	}
	var tcp []corev1.ServicePort
	for _, p := range svc.Spec.Ports {
		if p.Protocol != "" && p.Protocol != corev1.ProtocolTCP {
			plan.Warnings = append(plan.Warnings, fmt.Sprintf("port %d/%s skipped: Tailscale Services carry TCP only", p.Port, p.Protocol))
			continue
		}
		tcp = append(tcp, p)
	}
	appProto := func(p corev1.ServicePort) string {
		if p.AppProtocol == nil {
			return ""
		}
		return *p.AppProtocol
	}
	consumed := map[int32]bool{}
	if hostname != "" {
		for i := range tcp {
			ap := appProto(tcp[i])
			if httpAppProtocols[ap] || httpsAppProtocols[ap] {
				consumed[tcp[i].Port] = true
				if plan.HTTP == nil {
					plan.HTTP = &tcp[i]
					plan.HTTPS = httpsAppProtocols[ap]
				}
			}
		}
		if plan.HTTP == nil && len(tcp) > 0 {
			plan.HTTP = &tcp[0]
			consumed[tcp[0].Port] = true
		}
	}
	for _, p := range tcp {
		if !consumed[p.Port] {
			plan.TCP = append(plan.TCP, p)
		}
	}
	return plan
}

func (r *ServiceReconciler) objects(svc *corev1.Service, className string, plan ServicePlan) (*gwv1.Gateway, []*gwv1.HTTPRoute, []*gwv1.TCPRoute) {
	gw := &gwv1.Gateway{
		ObjectMeta: sourceMeta(svc, "Service", "v1", config.SourceService, svc.Name),
		Spec:       gwv1.GatewaySpec{GatewayClassName: gwv1.ObjectName(className)},
	}
	var httpRoutes []*gwv1.HTTPRoute
	var tcpRoutes []*gwv1.TCPRoute
	if p := plan.HTTP; p != nil {
		proto, lproto := "http", gwv1.HTTPProtocolType
		if plan.HTTPS {
			proto, lproto = "https", gwv1.HTTPSProtocolType
		}
		lname := gwv1.SectionName(fmt.Sprintf("%s-%d", proto, p.Port))
		l := gwv1.Listener{Name: lname, Protocol: lproto, Port: gwv1.PortNumber(p.Port)}
		if plan.HTTPS {
			l.TLS = tailscaleTLSOptions()
		}
		gw.Spec.Listeners = append(gw.Spec.Listeners, l)
		httpRoutes = append(httpRoutes, &gwv1.HTTPRoute{
			ObjectMeta: sourceMeta(svc, "Service", "v1", config.SourceService, fmt.Sprintf("%s-%s-%d", svc.Name, proto, p.Port)),
			Spec: gwv1.HTTPRouteSpec{
				CommonRouteSpec: gwv1.CommonRouteSpec{ParentRefs: []gwv1.ParentReference{{Name: gwv1.ObjectName(svc.Name), SectionName: ptr.To(lname)}}},
				Hostnames:       []gwv1.Hostname{gwv1.Hostname(plan.Label)},
				Rules: []gwv1.HTTPRouteRule{{
					Matches:     []gwv1.HTTPRouteMatch{{Path: &gwv1.HTTPPathMatch{Type: ptr.To(gwv1.PathMatchPathPrefix), Value: ptr.To("/")}}},
					BackendRefs: []gwv1.HTTPBackendRef{{BackendRef: gwv1.BackendRef{BackendObjectReference: gwv1.BackendObjectReference{Name: gwv1.ObjectName(svc.Name), Port: ptr.To(gwv1.PortNumber(p.Port))}}}},
				}},
			},
		})
	}
	for _, p := range plan.TCP {
		lname := gwv1.SectionName(fmt.Sprintf("tcp-%d", p.Port))
		gw.Spec.Listeners = append(gw.Spec.Listeners, gwv1.Listener{Name: lname, Protocol: gwv1.TCPProtocolType, Port: gwv1.PortNumber(p.Port)})
		rt := &gwv1.TCPRoute{
			ObjectMeta: sourceMeta(svc, "Service", "v1", config.SourceService, fmt.Sprintf("%s-tcp-%d", svc.Name, p.Port)),
			Spec: gwv1.TCPRouteSpec{
				CommonRouteSpec: gwv1.CommonRouteSpec{ParentRefs: []gwv1.ParentReference{{Name: gwv1.ObjectName(svc.Name), SectionName: ptr.To(lname)}}},
				Rules: []gwv1.TCPRouteRule{{BackendRefs: []gwv1.BackendRef{{BackendObjectReference: gwv1.BackendObjectReference{
					Name: gwv1.ObjectName(svc.Name), Port: ptr.To(gwv1.PortNumber(p.Port)),
				}}}}},
			},
		}
		rt.Annotations[config.AnnotationTailscaleService] = plan.Label
		tcpRoutes = append(tcpRoutes, rt)
	}
	return gw, httpRoutes, tcpRoutes
}

// SetupWithManager registers the reconciler.
func (r *ServiceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	b := ctrl.NewControllerManagedBy(mgr).
		Named("service").
		For(&corev1.Service{}).
		Owns(&gwv1.Gateway{}).
		Owns(&gwv1.HTTPRoute{})
	if r.Features.TCPRoute {
		b = b.Owns(&gwv1.TCPRoute{})
	}
	return b.Complete(r)
}
