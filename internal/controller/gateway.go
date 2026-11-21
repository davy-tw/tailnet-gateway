package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"

	"go.pkgs.davy.tw/tailnet-gateway/internal/config"
	"go.pkgs.davy.tw/tailnet-gateway/internal/proxy"
	"go.pkgs.davy.tw/tailnet-gateway/internal/serveconf"
	"go.pkgs.davy.tw/tailnet-gateway/internal/tailscale"
	"go.pkgs.davy.tw/tailnet-gateway/internal/translate"
)

// Requeue intervals.
const (
	pendingRequeue  = 5 * time.Second
	degradedRequeue = 30 * time.Second
	// deviceCheckAfter is how long a replica must be unready before the
	// controller checks whether its device still exists.
	deviceCheckAfter = 2 * time.Minute
)

// GatewayReconciler provisions the data plane for managed Gateways and
// writes Gateway, route and policy status.
type GatewayReconciler struct {
	classResolver
	Scheme   *runtime.Scheme
	TS       tailscale.API // nil in manual key mode
	Features Features
	Recorder events.EventRecorder
	Now      func() time.Time
}

// NewGatewayReconciler returns a reconciler. ts may be nil.
func NewGatewayReconciler(c client.Client, scheme *runtime.Scheme, cfg *config.Config, ts tailscale.API, f Features, rec events.EventRecorder) *GatewayReconciler {
	return &GatewayReconciler{
		classResolver: classResolver{Client: c, Config: cfg},
		Scheme:        scheme, TS: ts, Features: f, Recorder: rec, Now: time.Now,
	}
}

// pendingError is a provisioning step that cannot finish yet; it is
// reported on Programmed without being treated as a failure.
type pendingError struct{ msg string }

func (e *pendingError) Error() string { return e.msg }

func (r *GatewayReconciler) event(obj runtime.Object, typ, reason, msg string, args ...any) {
	if r.Recorder != nil {
		r.Recorder.Eventf(obj, nil, typ, reason, "Reconcile", msg, args...)
	}
}

// Reconcile implements reconcile.Reconciler.
func (r *GatewayReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	gw := &gwv1.Gateway{}
	if err := r.Get(ctx, req.NamespacedName, gw); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	gc, classExists, err := r.lookupClass(ctx, string(gw.Spec.GatewayClassName))
	if err != nil {
		return ctrl.Result{}, err
	}
	deleting := !gw.DeletionTimestamp.IsZero()
	if gc == nil || deleting {
		if !controllerutil.ContainsFinalizer(gw, config.Finalizer) {
			return ctrl.Result{}, nil
		}
		if !deleting && !classExists {
			// The class is gone, perhaps only for a moment (a reinstall,
			// or a GitOps tool recreating it). Stop serving but keep the
			// devices and state, as for an invalid class.
			return ctrl.Result{}, errors.Join(r.withdraw(ctx, gw), r.writeClassMissingStatus(ctx, gw),
				r.releaseListenerSets(ctx, gw, fmt.Sprintf("GatewayClass %q of the parent Gateway not found", gw.Spec.GatewayClassName)))
		}
		// The Gateway is being deleted, or now belongs to another
		// controller's class.
		done, err := r.finalize(ctx, gw, !deleting)
		if err != nil || !done {
			return ctrl.Result{RequeueAfter: pendingRequeue}, err
		}
		if deleting {
			if err := r.releaseListenerSets(ctx, gw, "the parent Gateway is being deleted"); err != nil {
				return ctrl.Result{}, err
			}
		}
		orig := gw.DeepCopy()
		controllerutil.RemoveFinalizer(gw, config.Finalizer)
		delete(gw.Annotations, config.AnnotationDevices)
		delete(gw.Annotations, config.AnnotationListenerSets)
		return ctrl.Result{}, client.IgnoreNotFound(r.Patch(ctx, gw, client.MergeFrom(orig)))
	}
	if !controllerutil.ContainsFinalizer(gw, config.Finalizer) {
		orig := gw.DeepCopy()
		controllerutil.AddFinalizer(gw, config.Finalizer)
		if err := r.Patch(ctx, gw, client.MergeFrom(orig)); err != nil {
			return ctrl.Result{}, err
		}
	}

	params, _, perr := r.classParams(ctx, gc)
	var ip *invalidParamsError
	if perr != nil && !asInvalid(perr, &ip) {
		return ctrl.Result{}, perr
	}

	routes, err := r.listRoutes(ctx)
	if err != nil {
		return ctrl.Result{}, err
	}
	sets, err := r.listListenerSets(ctx)
	if err != nil {
		return ctrl.Result{}, err
	}
	in, err := r.translateInput(ctx, gw, routes, sets.of(gw))
	if err != nil {
		return ctrl.Result{}, err
	}
	desired := params.Replicas
	if ip != nil {
		desired = 0
	}
	in.TailnetDomain = r.Config.TailnetDomain
	if in.TailnetDomain == "" {
		in.TailnetDomain = r.learnDomain(ctx, gw)
	}
	if in.ClaimedServices, err = r.claimedServices(ctx, gw); err != nil {
		return ctrl.Result{}, err
	}
	res := translate.Translate(in)

	// Route and policy status are written whatever happens next.
	statusErr := errors.Join(
		r.writeRouteStatus(ctx, gw, routes, res, sets),
		r.writePolicyStatus(ctx, gw, res),
		r.recordDevices(ctx, gw),
	)

	var progErr error
	var sts *appsv1.StatefulSet
	switch {
	case ip != nil:
		progErr = &pendingError{"GatewayClass parameters are invalid: " + ip.msg}
		statusErr = errors.Join(statusErr, r.withdraw(ctx, gw))
	case !res.Accepted:
		progErr = &pendingError{"Gateway is not accepted: " + res.AcceptedMessage}
		statusErr = errors.Join(statusErr, r.withdraw(ctx, gw))
	default:
		sts, progErr = r.provision(ctx, gw, params, res)
	}
	var pe *pendingError
	if progErr != nil && !errors.As(progErr, &pe) {
		logger.Error(progErr, "provisioning failed")
		r.event(gw, corev1.EventTypeWarning, "ProvisioningFailed", "%v", progErr)
	}

	ready := int32(0)
	if sts != nil {
		ready = sts.Status.ReadyReplicas
	}
	programmed, err := r.writeGatewayStatus(ctx, gw, res, ip, progErr, ready, desired, in.TailnetDomain)
	if err != nil {
		return ctrl.Result{}, err
	}
	statusErr = errors.Join(statusErr, r.writeListenerSetStatus(ctx, gw, sets.of(gw), res, ip))
	if r.Features.ListenerSet {
		statusErr = errors.Join(statusErr, r.releaseMovedListenerSets(ctx, gw, sets))
	}
	if statusErr != nil {
		return ctrl.Result{}, statusErr
	}
	if progErr != nil && pe == nil {
		return ctrl.Result{}, progErr
	}
	switch {
	case ip != nil || !res.Accepted:
		return ctrl.Result{}, nil // a spec or class change will trigger the next reconcile
	case !programmed || res.WebPending || pe != nil:
		return ctrl.Result{RequeueAfter: pendingRequeue}, nil
	case ready < desired:
		return ctrl.Result{RequeueAfter: degradedRequeue}, nil
	}
	return ctrl.Result{}, nil
}

// routeSet holds every route in the cluster, by kind.
type routeSet struct {
	http []*gwv1.HTTPRoute
	grpc []*gwv1.GRPCRoute
	tls  []*gwv1.TLSRoute
	tcp  []*gwv1.TCPRoute
	udp  []*gwv1.UDPRoute
}

func (r *GatewayReconciler) listRoutes(ctx context.Context) (*routeSet, error) {
	rs := &routeSet{}
	var hl gwv1.HTTPRouteList
	if err := r.List(ctx, &hl); err != nil {
		return nil, err
	}
	for i := range hl.Items {
		rs.http = append(rs.http, &hl.Items[i])
	}
	if r.Features.GRPCRoute {
		var l gwv1.GRPCRouteList
		if err := r.List(ctx, &l); err != nil {
			return nil, err
		}
		for i := range l.Items {
			rs.grpc = append(rs.grpc, &l.Items[i])
		}
	}
	if r.Features.TLSRoute {
		var l gwv1.TLSRouteList
		if err := r.List(ctx, &l); err != nil {
			return nil, err
		}
		for i := range l.Items {
			rs.tls = append(rs.tls, &l.Items[i])
		}
	}
	if r.Features.TCPRoute {
		var l gwv1.TCPRouteList
		if err := r.List(ctx, &l); err != nil {
			return nil, err
		}
		for i := range l.Items {
			rs.tcp = append(rs.tcp, &l.Items[i])
		}
	}
	if r.Features.UDPRoute {
		var l gwv1.UDPRouteList
		if err := r.List(ctx, &l); err != nil {
			return nil, err
		}
		for i := range l.Items {
			rs.udp = append(rs.udp, &l.Items[i])
		}
	}
	return rs, nil
}

// listenerSets holds every ListenerSet in the cluster.
type listenerSets map[types.NamespacedName]*gwv1.ListenerSet

func (r *GatewayReconciler) listListenerSets(ctx context.Context) (listenerSets, error) {
	out := listenerSets{}
	if !r.Features.ListenerSet {
		return out, nil
	}
	var l gwv1.ListenerSetList
	if err := r.List(ctx, &l); err != nil {
		return nil, err
	}
	for i := range l.Items {
		ls := &l.Items[i]
		out[types.NamespacedName{Namespace: ls.Namespace, Name: ls.Name}] = ls
	}
	return out, nil
}

// of returns the ListenerSets whose parentRef names gw, sorted by name.
func (s listenerSets) of(gw *gwv1.Gateway) []*gwv1.ListenerSet {
	var out []*gwv1.ListenerSet
	for _, ls := range s {
		if translate.ListenerSetTargets(ls, gw.Namespace, gw.Name) {
			out = append(out, ls)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Namespace+"/"+out[i].Name < out[j].Namespace+"/"+out[j].Name
	})
	return out
}

func (r *GatewayReconciler) translateInput(ctx context.Context, gw *gwv1.Gateway, rs *routeSet, sets []*gwv1.ListenerSet) (*translate.Input, error) {
	in := &translate.Input{
		Gateway:        gw,
		ControllerName: r.Config.ControllerName,
		HTTPRoutes:     rs.http, GRPCRoutes: rs.grpc, TLSRoutes: rs.tls, TCPRoutes: rs.tcp, UDPRoutes: rs.udp,
		ListenerSets:  sets,
		ClusterDomain: r.Config.ClusterDomain,
		Supported:     r.Features.SupportedRouteKinds(),
	}
	if r.Features.ReferenceGrant {
		var l gwv1.ReferenceGrantList
		if err := r.List(ctx, &l); err != nil {
			return nil, err
		}
		for i := range l.Items {
			in.ReferenceGrants = append(in.ReferenceGrants, &l.Items[i])
		}
	}
	if r.Features.BackendTLSPolicy {
		var l gwv1.BackendTLSPolicyList
		if err := r.List(ctx, &l); err != nil {
			return nil, err
		}
		for i := range l.Items {
			in.BackendTLSPolicies = append(in.BackendTLSPolicies, &l.Items[i])
		}
	}
	if needsNamespaceLabels(gw, sets) {
		var nl corev1.NamespaceList
		if err := r.List(ctx, &nl); err != nil {
			return nil, err
		}
		in.NamespaceLabels = map[string]map[string]string{}
		for _, ns := range nl.Items {
			in.NamespaceLabels[ns.Name] = ns.Labels
		}
	}
	in.GetService = func(ns, name string) *corev1.Service {
		svc := &corev1.Service{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, svc); err != nil {
			return nil
		}
		return svc
	}
	return in, nil
}

func needsNamespaceLabels(gw *gwv1.Gateway, sets []*gwv1.ListenerSet) bool {
	selects := func(ar *gwv1.AllowedRoutes) bool {
		return ar != nil && ar.Namespaces != nil && ar.Namespaces.From != nil && *ar.Namespaces.From == gwv1.NamespacesFromSelector
	}
	for _, l := range gw.Spec.Listeners {
		if selects(l.AllowedRoutes) {
			return true
		}
	}
	if len(sets) == 0 {
		return false
	}
	if al := gw.Spec.AllowedListeners; al != nil && al.Namespaces != nil && al.Namespaces.From != nil &&
		*al.Namespaces.From == gwv1.NamespacesFromSelector {
		return true
	}
	for _, ls := range sets {
		for _, l := range ls.Spec.Listeners {
			if selects(l.AllowedRoutes) {
				return true
			}
		}
	}
	return false
}

// claimedServices returns the Tailscale Service names that older Gateways
// hold, read from their serve config ConfigMaps: the names they serve and
// the ones waiting for the tailnet domain. Names are tailnet-wide: if two
// Gateways advertised the same name, its traffic would be spread over both
// Gateways' backends. The oldest Gateway keeps a name.
func (r *GatewayReconciler) claimedServices(ctx context.Context, gw *gwv1.Gateway) (map[string]string, error) {
	var cms corev1.ConfigMapList
	if err := r.List(ctx, &cms, client.MatchingLabels{proxy.LabelName: proxy.AppName, proxy.LabelManagedBy: proxy.ManagedBy}); err != nil {
		return nil, err
	}
	claimed := map[string]string{}
	for i := range cms.Items {
		cm := &cms.Items[i]
		owner := metav1.GetControllerOf(cm)
		if owner == nil || owner.Kind != translate.KindGateway || owner.UID == gw.UID {
			continue
		}
		other := &gwv1.Gateway{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: cm.Namespace, Name: owner.Name}, other); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return nil, err
		}
		if other.UID != owner.UID || !olderGateway(other, gw) || !r.isServeConfig(ctx, cm, other) {
			continue
		}
		labels := strings.Split(cm.Annotations[config.AnnotationTailscaleServices], ",")
		var sc serveconf.Config
		if err := json.Unmarshal([]byte(cm.Data[proxy.ServeConfigKey]), &sc); err == nil {
			labels = append(labels, sc.Labels()...)
		}
		for _, l := range labels {
			if l != "" {
				claimed[l] = other.Namespace + "/" + other.Name
			}
		}
	}
	return claimed, nil
}

// isServeConfig reports whether cm is the serve config this controller
// wrote for gw: named after it, of a class this controller serves, and
// applied by this controller. Anyone who may create ConfigMaps can copy
// the labels and owner reference, and must not reserve names that way.
func (r *GatewayReconciler) isServeConfig(ctx context.Context, cm *corev1.ConfigMap, gw *gwv1.Gateway) bool {
	if cm.Name != gw.Name {
		return false
	}
	if gc, err := r.managedClass(ctx, string(gw.Spec.GatewayClassName)); err != nil || gc == nil {
		return false
	}
	for _, f := range cm.ManagedFields {
		if f.Manager == FieldOwner && f.Operation == metav1.ManagedFieldsOperationApply {
			return true
		}
	}
	return false
}

// olderGateway orders Gateways by creation time, then by namespace/name.
func olderGateway(a, b *gwv1.Gateway) bool {
	if !a.CreationTimestamp.Equal(&b.CreationTimestamp) {
		return a.CreationTimestamp.Before(&b.CreationTimestamp)
	}
	return a.Namespace+"/"+a.Name < b.Namespace+"/"+b.Name
}

// learnDomain reads the tailnet MagicDNS suffix from any replica's state
// Secret, where the Tailscale container records its device FQDN.
func (r *GatewayReconciler) learnDomain(ctx context.Context, gw *gwv1.Gateway) string {
	secrets, err := r.stateSecrets(ctx, gw)
	if err != nil {
		return ""
	}
	for _, s := range secrets {
		if d := DomainFromFQDN(string(s.Data[proxy.KeyDeviceFQDN])); d != "" {
			return d
		}
	}
	return ""
}

// DomainFromFQDN returns the tailnet suffix of a device FQDN
// ("host.tail1234.ts.net." -> "tail1234.ts.net").
func DomainFromFQDN(fqdn string) string {
	fqdn = strings.TrimSuffix(strings.TrimSpace(fqdn), ".")
	_, rest, ok := strings.Cut(fqdn, ".")
	if !ok || !strings.Contains(rest, ".") {
		return ""
	}
	return strings.ToLower(rest)
}

func stateSecretLabels(gw string) map[string]string {
	l := proxy.Labels(gw)
	l[proxy.LabelComponent] = proxy.ComponentState
	return l
}

func (r *GatewayReconciler) stateSecrets(ctx context.Context, gw *gwv1.Gateway) ([]corev1.Secret, error) {
	var l corev1.SecretList
	if err := r.List(ctx, &l, client.InNamespace(gw.Namespace), client.MatchingLabels(stateSecretLabels(gw.Name))); err != nil {
		return nil, err
	}
	var out []corev1.Secret
	for _, s := range l.Items {
		if metav1.IsControlledBy(&s, gw) {
			out = append(out, s)
		}
	}
	return out, nil
}

func ownerRef(gw *gwv1.Gateway) metav1.OwnerReference {
	return metav1.OwnerReference{
		APIVersion:         gwv1.GroupVersion.String(),
		Kind:               translate.KindGateway,
		Name:               gw.Name,
		UID:                gw.UID,
		Controller:         ptr.To(true),
		BlockOwnerDeletion: ptr.To(true),
	}
}

func (r *GatewayReconciler) provision(ctx context.Context, gw *gwv1.Gateway, params config.ClassParams, res *translate.Result) (*appsv1.StatefulSet, error) {
	if len(gw.Name) > maxGatewayNameLength {
		return nil, &pendingError{fmt.Sprintf("Gateway names longer than %d characters are not supported: "+
			"the name is used for the proxy StatefulSet and as a label value", maxGatewayNameLength)}
	}
	spec := &proxy.Spec{
		Namespace: gw.Namespace,
		Gateway:   gw.Name,
		Params:    params,
		PodLabels: infrastructureLabels(gw),
		Owner:     ownerRef(gw),
	}
	cfg, err := res.Config.Marshal()
	if err != nil {
		return nil, err
	}
	cm := spec.ConfigMap(cfg)
	services := append([]string(nil), res.Services...)
	sort.Strings(services)
	cm.Annotations = map[string]string{config.AnnotationTailscaleServices: strings.Join(services, ",")}
	for _, obj := range []client.Object{spec.ServiceAccount(), spec.Role(), spec.RoleBinding(), cm} {
		if err := r.applyOwned(ctx, gw, obj); err != nil {
			return nil, err
		}
	}
	if pdb := spec.PodDisruptionBudget(); pdb != nil {
		if err := r.applyOwned(ctx, gw, pdb); err != nil {
			return nil, fmt.Errorf("applying PodDisruptionBudget: %w", err)
		}
	} else {
		log.FromContext(ctx).Info("one proxy replica: no PodDisruptionBudget, so a node drain takes the Gateway offline")
		if err := r.deleteOwned(ctx, gw, &policyv1.PodDisruptionBudget{}, gw.Name); err != nil {
			return nil, err
		}
	}
	if err := r.ensureStateSecrets(ctx, gw, params); err != nil {
		return r.currentStatefulSet(ctx, gw), err
	}
	if err := r.applyOwned(ctx, gw, spec.StatefulSet()); err != nil {
		return nil, err
	}
	if err := r.pruneStateSecrets(ctx, gw, params.Replicas); err != nil {
		return nil, err
	}
	return r.currentStatefulSet(ctx, gw), nil
}

// maxGatewayNameLength keeps the proxy StatefulSet's pods valid: their
// controller-revision-hash label is "<name>-<10-character hash>", and label
// values are limited to 63 characters.
const maxGatewayNameLength = 52

// applyOwned applies an object of the Gateway's data plane. An object of
// the same name that the Gateway does not control is left alone: taking it
// over would make it a child of the Gateway, and garbage collection would
// delete it with the Gateway.
func (r *GatewayReconciler) applyOwned(ctx context.Context, gw *gwv1.Gateway, obj client.Object) error {
	err := applyControlled(ctx, r.Client, r.reader(), r.Scheme, obj, gw)
	var ce *conflictError
	if errors.As(err, &ce) {
		return &pendingError{strings.Replace(ce.msg, "this object", "this Gateway", 1)}
	}
	return err
}

// infrastructureLabels are the labels the Gateway asks for on its
// generated resources (spec.infrastructure.labels). The Gateway's own
// labels are not copied: they could make the proxy pods match the selector
// of an unrelated Service.
func infrastructureLabels(gw *gwv1.Gateway) map[string]string {
	if gw.Spec.Infrastructure == nil {
		return nil
	}
	out := map[string]string{}
	for k, v := range gw.Spec.Infrastructure.Labels {
		out[string(k)] = string(v)
	}
	return out
}

// withdraw stops serving a Gateway that is no longer accepted: its proxy
// StatefulSet is deleted (the pods drain on the way out). State Secrets are
// kept, so the replicas come back as the same devices once it is accepted
// again.
func (r *GatewayReconciler) withdraw(ctx context.Context, gw *gwv1.Gateway) error {
	return r.deleteOwned(ctx, gw, &appsv1.StatefulSet{}, gw.Name)
}

func (r *GatewayReconciler) currentStatefulSet(ctx context.Context, gw *gwv1.Gateway) *appsv1.StatefulSet {
	sts := &appsv1.StatefulSet{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: gw.Namespace, Name: gw.Name}, sts); err != nil {
		return nil
	}
	return sts
}

func (r *GatewayReconciler) deleteOwned(ctx context.Context, gw *gwv1.Gateway, obj client.Object, name string) error {
	if err := r.Get(ctx, types.NamespacedName{Namespace: gw.Namespace, Name: name}, obj); err != nil {
		return client.IgnoreNotFound(err)
	}
	if !metav1.IsControlledBy(obj, gw) {
		return nil
	}
	return client.IgnoreNotFound(r.Delete(ctx, obj))
}

// authKey returns a key for a new replica: minted through the API, or
// copied from the user's Secret named after the Gateway.
func (r *GatewayReconciler) authKey(ctx context.Context, gw *gwv1.Gateway, params config.ClassParams) (string, error) {
	if r.TS != nil {
		key, err := r.TS.CreateAuthKey(ctx, tailscale.KeyRequest{Tags: params.Tags, Description: r.Config.AuthKeyDescription})
		if err != nil {
			return "", fmt.Errorf("creating auth key: %w", err)
		}
		return key, nil
	}
	// Read directly: only the controller's own Secrets are cached.
	user := &corev1.Secret{}
	if err := r.reader().Get(ctx, types.NamespacedName{Namespace: gw.Namespace, Name: gw.Name}, user); err != nil {
		if apierrors.IsNotFound(err) {
			return "", &pendingError{fmt.Sprintf("no auth key: create Secret %s/%s with an %q (a reusable, ephemeral, pre-authorized, tagged key), or configure an OAuth client", gw.Namespace, gw.Name, proxy.KeyAuthKey)}
		}
		return "", err
	}
	key := strings.TrimSpace(string(user.Data[proxy.KeyAuthKey]))
	if key == "" {
		return "", &pendingError{fmt.Sprintf("Secret %s/%s has no %q key", gw.Namespace, gw.Name, proxy.KeyAuthKey)}
	}
	return key, nil
}

// ensureStateSecrets makes sure every replica has a state Secret, seeded
// with an auth key until the replica has a device identity.
func (r *GatewayReconciler) ensureStateSecrets(ctx context.Context, gw *gwv1.Gateway, params config.ClassParams) error {
	for i := 0; i < int(params.Replicas); i++ {
		name := proxy.StateSecretName(gw.Name, i)
		sec := &corev1.Secret{}
		nn := types.NamespacedName{Namespace: gw.Namespace, Name: name}
		err := r.Get(ctx, nn, sec)
		if apierrors.IsNotFound(err) {
			// The cache only holds Secrets labelled as this controller's:
			// check that no other Secret has the name before minting a key.
			err = r.reader().Get(ctx, nn, sec)
		}
		switch {
		case apierrors.IsNotFound(err):
			key, err := r.authKey(ctx, gw, params)
			if err != nil {
				return err
			}
			sec = &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name: name, Namespace: gw.Namespace,
					Labels:          stateSecretLabels(gw.Name),
					OwnerReferences: []metav1.OwnerReference{ownerRef(gw)},
				},
				Type: corev1.SecretTypeOpaque,
				Data: map[string][]byte{proxy.KeyAuthKey: []byte(key)},
			}
			if err := r.Create(ctx, sec); err != nil && !apierrors.IsAlreadyExists(err) {
				return fmt.Errorf("creating state Secret %s: %w", name, err)
			}
			continue
		case err != nil:
			return err
		}
		if !metav1.IsControlledBy(sec, gw) {
			return &pendingError{fmt.Sprintf("Secret %s/%s exists and is not owned by this Gateway", gw.Namespace, name)}
		}
		if len(sec.Data[proxy.KeyDeviceID]) > 0 {
			if err := r.checkDevice(ctx, gw, params, sec, i); err != nil {
				return err
			}
			continue
		}
		if len(sec.Data[proxy.KeyAuthKey]) > 0 {
			continue // never overwrite a key that is already there
		}
		key, err := r.authKey(ctx, gw, params)
		if err != nil {
			return err
		}
		orig := sec.DeepCopy()
		if sec.Data == nil {
			sec.Data = map[string][]byte{}
		}
		sec.Data[proxy.KeyAuthKey] = []byte(key)
		if err := r.Patch(ctx, sec, client.MergeFrom(orig)); err != nil {
			return fmt.Errorf("seeding state Secret %s: %w", name, err)
		}
	}
	return nil
}

// checkDevice recovers a replica whose device was removed from the tailnet
// (for example an ephemeral device that stayed offline too long): its state
// is cleared and reseeded, and the pod restarted.
func (r *GatewayReconciler) checkDevice(ctx context.Context, gw *gwv1.Gateway, params config.ClassParams, sec *corev1.Secret, i int) error {
	if r.TS == nil {
		return nil
	}
	pod := &corev1.Pod{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: gw.Namespace, Name: sec.Name}, pod); err != nil {
		return client.IgnoreNotFound(err)
	}
	var readyCond *corev1.PodCondition
	for j := range pod.Status.Conditions {
		if pod.Status.Conditions[j].Type == corev1.PodReady {
			readyCond = &pod.Status.Conditions[j]
		}
	}
	if readyCond == nil || readyCond.Status == corev1.ConditionTrue || r.Now().Sub(readyCond.LastTransitionTime.Time) < deviceCheckAfter {
		return nil
	}
	id := string(sec.Data[proxy.KeyDeviceID])
	exists, err := r.TS.DeviceExists(ctx, id)
	if err != nil {
		// Only a recovery check: a failure (such as an OAuth client
		// without device read access) must not stop provisioning.
		log.FromContext(ctx).Info("could not check whether the device of an unready replica exists", "replica", i, "device", id, "error", err.Error())
		return nil
	}
	if exists {
		return nil
	}
	log.FromContext(ctx).Info("device of replica no longer exists; resetting its state", "replica", i, "device", id)
	key, err := r.authKey(ctx, gw, params)
	if err != nil {
		return err
	}
	sec.Data = map[string][]byte{proxy.KeyAuthKey: []byte(key)}
	if err := r.Update(ctx, sec); err != nil {
		return err
	}
	r.event(gw, corev1.EventTypeNormal, "DeviceReset", "replica %d's device %s was removed from the tailnet; re-registering", i, id)
	if err := r.forgetDevices(ctx, gw, id); err != nil {
		return err
	}
	return client.IgnoreNotFound(r.Delete(ctx, pod))
}

// pruneStateSecrets removes the state (and devices) of replicas beyond the
// desired count.
func (r *GatewayReconciler) pruneStateSecrets(ctx context.Context, gw *gwv1.Gateway, replicas int32) error {
	secrets, err := r.stateSecrets(ctx, gw)
	if err != nil {
		return err
	}
	for i := range secrets {
		s := &secrets[i]
		ord, err := strconv.Atoi(strings.TrimPrefix(s.Name, gw.Name+"-"))
		if err != nil || ord < int(replicas) {
			continue
		}
		// Wait for the replica to finish draining before removing its
		// device and state.
		if err := r.Get(ctx, types.NamespacedName{Namespace: gw.Namespace, Name: s.Name}, &corev1.Pod{}); err == nil {
			continue
		} else if !apierrors.IsNotFound(err) {
			return err
		}
		if err := r.deleteDevice(ctx, gw, string(s.Data[proxy.KeyDeviceID])); err != nil {
			return err
		}
		if err := r.forgetDevices(ctx, gw, string(s.Data[proxy.KeyDeviceID])); err != nil {
			return err
		}
		if err := r.Delete(ctx, s); client.IgnoreNotFound(err) != nil {
			return err
		}
	}
	return nil
}

func (r *GatewayReconciler) deleteDevice(ctx context.Context, gw *gwv1.Gateway, id string) error {
	if id == "" {
		return nil
	}
	if r.TS == nil {
		log.FromContext(ctx).Info("manual key mode: not deleting device; ephemeral devices expire on their own", "device", id, "gateway", gw.Name)
		return nil
	}
	if err := r.TS.DeleteDevice(ctx, id); err != nil {
		return fmt.Errorf("deleting device %s: %w", id, err)
	}
	return nil
}

// finalize cleans up a Gateway that is being deleted, or that stopped
// being managed (unmanage). The proxy StatefulSet goes first, and the
// devices are deleted only once its pods are gone, so that the replicas
// drain while their devices still serve. It reports whether cleanup is
// done; until then the caller requeues.
func (r *GatewayReconciler) finalize(ctx context.Context, gw *gwv1.Gateway, unmanage bool) (bool, error) {
	secrets, err := r.stateSecrets(ctx, gw)
	if err != nil {
		return false, err
	}
	if err := r.withdraw(ctx, gw); err != nil {
		return false, err
	}
	if running, err := r.proxyPodsRunning(ctx, gw); err != nil || running {
		return false, err
	}
	// Device IDs come from the state Secrets and from the Gateway's
	// annotation, which outlives the Secrets under foreground deletion.
	ids := recordedDevices(gw)
	for _, s := range secrets {
		ids = addDevice(ids, string(s.Data[proxy.KeyDeviceID]))
	}
	if r.TS == nil && len(ids) > 0 {
		r.event(gw, corev1.EventTypeWarning, "DevicesNotDeleted", "no OAuth client configured: devices are not deleted (ephemeral devices expire on their own)")
	}
	for _, id := range ids {
		if err := r.deleteDevice(ctx, gw, id); err != nil {
			return false, err
		}
	}
	routes, err := r.listRoutes(ctx)
	if err != nil {
		return false, err
	}
	sets, err := r.listListenerSets(ctx)
	if err != nil {
		return false, err
	}
	if err := r.writeRouteStatus(ctx, gw, routes, &translate.Result{}, sets); err != nil {
		return false, err
	}
	if err := r.writePolicyStatus(ctx, gw, &translate.Result{}); err != nil {
		return false, err
	}
	if !unmanage {
		return true, nil // owner references garbage-collect the rest
	}
	for _, o := range []client.Object{&policyv1.PodDisruptionBudget{}, &corev1.ConfigMap{},
		&rbacv1.RoleBinding{}, &rbacv1.Role{}, &corev1.ServiceAccount{}} {
		if err := r.deleteOwned(ctx, gw, o, gw.Name); err != nil {
			return false, err
		}
	}
	for i := range secrets {
		if err := r.Delete(ctx, &secrets[i]); client.IgnoreNotFound(err) != nil {
			return false, err
		}
	}
	return true, nil
}

// proxyPodsRunning reports whether any pod of the Gateway's proxy
// StatefulSet still exists.
func (r *GatewayReconciler) proxyPodsRunning(ctx context.Context, gw *gwv1.Gateway) (bool, error) {
	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(gw.Namespace), client.MatchingLabels(proxy.SelectorLabels(gw.Name))); err != nil {
		return false, err
	}
	for i := range pods.Items {
		if o := metav1.GetControllerOf(&pods.Items[i]); o != nil && o.Kind == "StatefulSet" && o.Name == gw.Name {
			return true, nil
		}
	}
	return false, nil
}

// recordedDevices returns the device IDs in the Gateway's annotation.
func recordedDevices(gw *gwv1.Gateway) []string {
	var ids []string
	for _, id := range strings.Split(gw.Annotations[config.AnnotationDevices], ",") {
		ids = addDevice(ids, strings.TrimSpace(id))
	}
	return ids
}

func addDevice(ids []string, id string) []string {
	if id == "" {
		return ids
	}
	for _, x := range ids {
		if x == id {
			return ids
		}
	}
	ids = append(ids, id)
	sort.Strings(ids)
	return ids
}

// recordDevices adds the device IDs found in the state Secrets to the
// Gateway's annotation.
func (r *GatewayReconciler) recordDevices(ctx context.Context, gw *gwv1.Gateway) error {
	secrets, err := r.stateSecrets(ctx, gw)
	if err != nil {
		return err
	}
	ids := recordedDevices(gw)
	for _, s := range secrets {
		ids = addDevice(ids, string(s.Data[proxy.KeyDeviceID]))
	}
	return r.setRecordedDevices(ctx, gw, ids)
}

// forgetDevices removes device IDs that no longer need deleting.
func (r *GatewayReconciler) forgetDevices(ctx context.Context, gw *gwv1.Gateway, forget ...string) error {
	var ids []string
	for _, id := range recordedDevices(gw) {
		keep := true
		for _, f := range forget {
			keep = keep && id != f
		}
		if keep {
			ids = append(ids, id)
		}
	}
	return r.setRecordedDevices(ctx, gw, ids)
}

func (r *GatewayReconciler) setRecordedDevices(ctx context.Context, gw *gwv1.Gateway, ids []string) error {
	value := strings.Join(ids, ",")
	if gw.Annotations[config.AnnotationDevices] == value {
		return nil
	}
	orig := gw.DeepCopy()
	if value == "" {
		delete(gw.Annotations, config.AnnotationDevices)
	} else {
		if gw.Annotations == nil {
			gw.Annotations = map[string]string{}
		}
		gw.Annotations[config.AnnotationDevices] = value
	}
	return r.Patch(ctx, gw, client.MergeFromWithOptions(orig, client.MergeFromWithOptimisticLock{}))
}

// writeClassMissingStatus marks a Gateway whose GatewayClass no longer
// exists as not accepted and not programmed.
func (r *GatewayReconciler) writeClassMissingStatus(ctx context.Context, gw *gwv1.Gateway) error {
	orig := gw.DeepCopy()
	msg := fmt.Sprintf("GatewayClass %q not found; the proxy is stopped and its devices are kept until the class is back", gw.Spec.GatewayClassName)
	for _, t := range []gwv1.GatewayConditionType{gwv1.GatewayConditionAccepted, gwv1.GatewayConditionProgrammed} {
		meta.SetStatusCondition(&gw.Status.Conditions, metav1.Condition{Type: string(t), Status: metav1.ConditionFalse,
			Reason: string(gwv1.GatewayReasonPending), Message: msg, ObservedGeneration: gw.Generation})
	}
	gw.Status.Addresses = nil
	if gatewayStatusEqual(&orig.Status, &gw.Status) {
		return nil
	}
	return client.IgnoreNotFound(r.Status().Patch(ctx, gw, client.MergeFrom(orig)))
}

// ---- Gateway status ----

func (r *GatewayReconciler) writeGatewayStatus(ctx context.Context, gw *gwv1.Gateway, res *translate.Result, ip *invalidParamsError,
	progErr error, ready, desired int32, domain string) (bool, error) {
	orig := gw.DeepCopy()
	st := &gw.Status
	gen := gw.Generation

	accepted := metav1.Condition{Type: string(gwv1.GatewayConditionAccepted), ObservedGeneration: gen}
	switch {
	case ip != nil:
		accepted.Status, accepted.Reason, accepted.Message = metav1.ConditionFalse, string(gwv1.GatewayReasonInvalidParameters), ip.msg
	case !res.Accepted:
		accepted.Status, accepted.Reason, accepted.Message = metav1.ConditionFalse, res.AcceptedReason, res.AcceptedMessage
	default:
		accepted.Status, accepted.Reason, accepted.Message = metav1.ConditionTrue, res.AcceptedReason, res.AcceptedMessage
	}
	meta.SetStatusCondition(&st.Conditions, accepted)

	counts := fmt.Sprintf("%d/%d proxy replicas ready", ready, desired)
	programmed := metav1.Condition{Type: string(gwv1.GatewayConditionProgrammed), ObservedGeneration: gen,
		Status: metav1.ConditionFalse, Reason: string(gwv1.GatewayReasonPending)}
	switch {
	case ip != nil || !res.Accepted:
		programmed.Reason = string(gwv1.GatewayReasonInvalid)
		programmed.Message = progErr.Error()
	case progErr != nil && ready == 0:
		programmed.Message = fmt.Sprintf("%v (%s)", progErr, counts)
	case ready == 0:
		programmed.Message = "waiting for a proxy replica to become ready (" + counts + ")"
	case res.WebPending:
		programmed.Message = "waiting to learn the tailnet domain from a proxy replica (" + counts + ")"
	default:
		// Ready replicas keep serving through a failed step (a key that
		// could not be minted while scaling up, an API rate limit), so the
		// Gateway stays programmed and keeps its addresses; the error is
		// reported in the message.
		programmed.Status = metav1.ConditionTrue
		programmed.Reason = string(gwv1.GatewayReasonProgrammed)
		programmed.Message = counts
		if progErr != nil {
			programmed.Message = fmt.Sprintf("%s; last update failed: %v", counts, progErr)
		}
	}
	meta.SetStatusCondition(&st.Conditions, programmed)

	var listeners []gwv1.ListenerStatus
	for _, lr := range res.Listeners {
		var prev []metav1.Condition
		for _, old := range orig.Status.Listeners {
			if old.Name == lr.Name {
				prev = old.Conditions
			}
		}
		conds, kinds := listenerConditions(prev, lr, programmed, gen)
		listeners = append(listeners, gwv1.ListenerStatus{Name: lr.Name, SupportedKinds: kinds, AttachedRoutes: lr.AttachedRoutes, Conditions: conds})
	}
	st.Listeners = listeners
	if r.Features.ListenerSet {
		n := res.AttachedListenerSets
		if ip != nil {
			n = 0 // see writeListenerSetStatus
		}
		st.AttachedListenerSets = ptr.To(n)
	}

	st.Addresses = nil
	if domain != "" && programmed.Status == metav1.ConditionTrue {
		for _, label := range res.Services {
			st.Addresses = append(st.Addresses, gwv1.GatewayStatusAddress{
				Type: ptr.To(gwv1.HostnameAddressType), Value: label + "." + domain,
			})
		}
	}
	isProgrammed := programmed.Status == metav1.ConditionTrue
	if gatewayStatusEqual(&orig.Status, st) {
		return isProgrammed, nil
	}
	return isProgrammed, r.Status().Patch(ctx, gw, client.MergeFrom(orig))
}

// listenerConditions merges a listener's translated conditions into its
// previous ones and adds Programmed, which follows the parent's programmed
// condition. It also returns the supported kinds, never nil.
func listenerConditions(prev []metav1.Condition, lr translate.ListenerResult, programmed metav1.Condition, gen int64) ([]metav1.Condition, []gwv1.RouteGroupKind) {
	var conds []metav1.Condition
	for _, c := range prev {
		if meta.FindStatusCondition(lr.Conditions, c.Type) != nil || c.Type == string(gwv1.ListenerConditionProgrammed) {
			conds = append(conds, c)
		}
	}
	for _, c := range lr.Conditions {
		meta.SetStatusCondition(&conds, c)
	}
	lp := metav1.Condition{Type: string(gwv1.ListenerConditionProgrammed), ObservedGeneration: gen,
		Status: programmed.Status, Reason: string(gwv1.ListenerReasonProgrammed), Message: programmed.Message}
	switch {
	case !lr.Valid:
		lp.Status, lp.Reason, lp.Message = metav1.ConditionFalse, lr.InvalidReason, "listener is not valid"
		if lp.Reason == "" {
			lp.Reason = string(gwv1.ListenerReasonInvalid)
		}
	case programmed.Status != metav1.ConditionTrue:
		lp.Status = metav1.ConditionFalse
		lp.Reason = string(gwv1.ListenerReasonPending)
	}
	meta.SetStatusCondition(&conds, lp)
	kinds := lr.SupportedKinds
	if kinds == nil {
		kinds = []gwv1.RouteGroupKind{}
	}
	return conds, kinds
}

func gatewayStatusEqual(a, b *gwv1.GatewayStatus) bool {
	if !equalConditions(a.Conditions, b.Conditions) || len(a.Listeners) != len(b.Listeners) || len(a.Addresses) != len(b.Addresses) ||
		ptr.Deref(a.AttachedListenerSets, -1) != ptr.Deref(b.AttachedListenerSets, -1) {
		return false
	}
	for i := range a.Addresses {
		if a.Addresses[i].Value != b.Addresses[i].Value {
			return false
		}
	}
	for i := range a.Listeners {
		x, y := a.Listeners[i], b.Listeners[i]
		if x.Name != y.Name || x.AttachedRoutes != y.AttachedRoutes || len(x.SupportedKinds) != len(y.SupportedKinds) || !equalConditions(x.Conditions, y.Conditions) {
			return false
		}
		for j := range x.SupportedKinds {
			if x.SupportedKinds[j].Kind != y.SupportedKinds[j].Kind {
				return false
			}
		}
	}
	return true
}

// ---- route and policy status ----

type routeStatusTarget struct {
	obj    client.Object
	kind   string
	status *gwv1.RouteStatus
}

func (rs *routeSet) targets() []routeStatusTarget {
	var out []routeStatusTarget
	for _, r := range rs.http {
		out = append(out, routeStatusTarget{r, translate.KindHTTPRoute, &r.Status.RouteStatus})
	}
	for _, r := range rs.grpc {
		out = append(out, routeStatusTarget{r, translate.KindGRPCRoute, &r.Status.RouteStatus})
	}
	for _, r := range rs.tls {
		out = append(out, routeStatusTarget{r, translate.KindTLSRoute, &r.Status.RouteStatus})
	}
	for _, r := range rs.tcp {
		out = append(out, routeStatusTarget{r, translate.KindTCPRoute, &r.Status.RouteStatus})
	}
	for _, r := range rs.udp {
		out = append(out, routeStatusTarget{r, translate.KindUDPRoute, &r.Status.RouteStatus})
	}
	return out
}

// writeRouteStatus rewrites this controller's entries for this Gateway in
// every route's status: results from res, and removal where a route no
// longer attaches.
//
// Entries for a ListenerSet belong to the Gateway the ListenerSet names. An
// entry of this controller for a ListenerSet that no Gateway of this
// controller serves any more (deleted, or moved to another Gateway) is
// removed by whichever Gateway's reconcile comes across it.
func (r *GatewayReconciler) writeRouteStatus(ctx context.Context, gw *gwv1.Gateway, rs *routeSet, res *translate.Result, sets listenerSets) error {
	results := map[translate.RouteRef]translate.RouteResult{}
	for _, rr := range res.Routes {
		results[rr.Ref] = rr
	}
	own := map[types.NamespacedName]bool{}
	for _, ls := range sets.of(gw) {
		own[types.NamespacedName{Namespace: ls.Namespace, Name: ls.Name}] = true
	}
	served := map[types.NamespacedName]bool{}
	servedElsewhere := func(key types.NamespacedName) bool {
		if v, ok := served[key]; ok {
			return v
		}
		v := false
		if ls := sets[key]; ls != nil {
			if parent, ok := listenerSetGateway(ls); ok {
				other := &gwv1.Gateway{}
				if err := r.Get(ctx, parent, other); err == nil {
					gc, err := r.managedClass(ctx, string(other.Spec.GatewayClassName))
					// When in doubt, leave the entry to that Gateway.
					v = err != nil || gc != nil
				} else {
					v = !apierrors.IsNotFound(err)
				}
			}
		}
		served[key] = v
		return v
	}
	var errs []error
	for _, t := range rs.targets() {
		ref := translate.RouteRef{Kind: t.kind, Namespace: t.obj.GetNamespace(), Name: t.obj.GetName()}
		result, has := results[ref]
		ours := func(p gwv1.RouteParentStatus) bool {
			if string(p.ControllerName) != r.Config.ControllerName {
				return false
			}
			if key, ok := listenerSetParentRef(p.ParentRef, ref.Namespace); ok {
				return own[key] || !servedElsewhere(key)
			}
			return parentRefTargets(p.ParentRef, ref.Namespace, gw.Namespace, gw.Name)
		}
		hadOurs := false
		var parents []gwv1.RouteParentStatus
		var old []gwv1.RouteParentStatus
		for _, p := range t.status.Parents {
			if ours(p) {
				hadOurs = true
				old = append(old, p)
				continue
			}
			parents = append(parents, p)
		}
		if !has && !hadOurs {
			continue
		}
		for _, pr := range result.Parents {
			var conds []metav1.Condition
			for _, o := range old {
				if parentRefEqual(o.ParentRef, pr.ParentRef) {
					conds = o.Conditions
				}
			}
			var keep []metav1.Condition
			for _, c := range conds {
				if meta.FindStatusCondition(pr.Conditions, c.Type) != nil {
					keep = append(keep, c)
				}
			}
			for _, c := range pr.Conditions {
				meta.SetStatusCondition(&keep, c)
			}
			parents = append(parents, gwv1.RouteParentStatus{
				ParentRef: pr.ParentRef, ControllerName: gwv1.GatewayController(r.Config.ControllerName), Conditions: keep,
			})
		}
		if routeParentsEqual(t.status.Parents, parents) {
			continue
		}
		orig := t.obj.DeepCopyObject().(client.Object)
		t.status.Parents = parents
		if t.status.Parents == nil {
			t.status.Parents = []gwv1.RouteParentStatus{}
		}
		if err := r.Status().Patch(ctx, t.obj, client.MergeFromWithOptions(orig, client.MergeFromWithOptimisticLock{})); err != nil && !apierrors.IsNotFound(err) {
			errs = append(errs, fmt.Errorf("%s status: %w", ref, err))
		}
	}
	return errors.Join(errs...)
}

func parentRefEqual(a, b gwv1.ParentReference) bool {
	str := func(p *string) string {
		if p == nil {
			return ""
		}
		return *p
	}
	g := func(p *gwv1.Group) *string {
		if p == nil {
			return nil
		}
		s := string(*p)
		return &s
	}
	k := func(p *gwv1.Kind) *string {
		if p == nil {
			return nil
		}
		s := string(*p)
		return &s
	}
	ns := func(p *gwv1.Namespace) *string {
		if p == nil {
			return nil
		}
		s := string(*p)
		return &s
	}
	sn := func(p *gwv1.SectionName) *string {
		if p == nil {
			return nil
		}
		s := string(*p)
		return &s
	}
	port := func(p *gwv1.PortNumber) int32 {
		if p == nil {
			return 0
		}
		return int32(*p)
	}
	return str(g(a.Group)) == str(g(b.Group)) && str(k(a.Kind)) == str(k(b.Kind)) && str(ns(a.Namespace)) == str(ns(b.Namespace)) &&
		a.Name == b.Name && str(sn(a.SectionName)) == str(sn(b.SectionName)) && port(a.Port) == port(b.Port)
}

func routeParentsEqual(a, b []gwv1.RouteParentStatus) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].ControllerName != b[i].ControllerName || !parentRefEqual(a[i].ParentRef, b[i].ParentRef) || !equalConditions(a[i].Conditions, b[i].Conditions) {
			return false
		}
	}
	return true
}

func (r *GatewayReconciler) writePolicyStatus(ctx context.Context, gw *gwv1.Gateway, res *translate.Result) error {
	if !r.Features.BackendTLSPolicy {
		return nil
	}
	results := map[types.NamespacedName]translate.PolicyResult{}
	for _, pr := range res.Policies {
		results[pr.Ref] = pr
	}
	var list gwv1.BackendTLSPolicyList
	if err := r.List(ctx, &list); err != nil {
		return err
	}
	ancestor := gatewayParentRef(gw)
	var errs []error
	for i := range list.Items {
		p := &list.Items[i]
		key := types.NamespacedName{Namespace: p.Namespace, Name: p.Name}
		result, has := results[key]
		var ancestors []gwv1.PolicyAncestorStatus
		var old *gwv1.PolicyAncestorStatus
		for j, a := range p.Status.Ancestors {
			if string(a.ControllerName) == r.Config.ControllerName && parentRefTargets(a.AncestorRef, p.Namespace, gw.Namespace, gw.Name) {
				old = &p.Status.Ancestors[j]
				continue
			}
			ancestors = append(ancestors, a)
		}
		if !has && old == nil {
			continue
		}
		if has {
			var conds []metav1.Condition
			if old != nil {
				conds = old.Conditions
			}
			for _, c := range result.Conditions {
				meta.SetStatusCondition(&conds, c)
			}
			ancestors = append(ancestors, gwv1.PolicyAncestorStatus{
				AncestorRef: ancestor, ControllerName: gwv1.GatewayController(r.Config.ControllerName), Conditions: conds,
			})
		}
		if old != nil && has && equalConditions(old.Conditions, ancestors[len(ancestors)-1].Conditions) && len(ancestors) == len(p.Status.Ancestors) {
			continue
		}
		orig := p.DeepCopy()
		if ancestors == nil {
			ancestors = []gwv1.PolicyAncestorStatus{}
		}
		p.Status.Ancestors = ancestors
		if err := r.Status().Patch(ctx, p, client.MergeFromWithOptions(orig, client.MergeFromWithOptimisticLock{})); err != nil && !apierrors.IsNotFound(err) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// ---- watches ----

func (r *GatewayReconciler) managedGateways(ctx context.Context) []reconcile.Request {
	var list gwv1.GatewayList
	if err := r.List(ctx, &list); err != nil {
		return nil
	}
	classes := map[string]bool{}
	var out []reconcile.Request
	for _, gw := range list.Items {
		name := string(gw.Spec.GatewayClassName)
		managed, seen := classes[name]
		if !seen {
			gc, _ := r.managedClass(ctx, name)
			managed = gc != nil
			classes[name] = managed
		}
		if managed || controllerutil.ContainsFinalizer(&gw, config.Finalizer) {
			out = append(out, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: gw.Namespace, Name: gw.Name}})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].String() < out[j].String() })
	return out
}

func (r *GatewayReconciler) allGateways(ctx context.Context, _ client.Object) []reconcile.Request {
	return r.managedGateways(ctx)
}

func (r *GatewayReconciler) gatewaysForClass(ctx context.Context, obj client.Object) []reconcile.Request {
	var list gwv1.GatewayList
	if err := r.List(ctx, &list); err != nil {
		return nil
	}
	var out []reconcile.Request
	for _, gw := range list.Items {
		if string(gw.Spec.GatewayClassName) == obj.GetName() {
			out = append(out, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: gw.Namespace, Name: gw.Name}})
		}
	}
	return out
}

func (r *GatewayReconciler) gatewaysForParamsConfigMap(ctx context.Context, obj client.Object) []reconcile.Request {
	if l := obj.GetLabels(); l[proxy.LabelName] == proxy.AppName && l[proxy.LabelManagedBy] == proxy.ManagedBy {
		// A serve config changed: other Gateways may gain or lose a
		// Tailscale Service name (see claimedServices).
		return r.managedGateways(ctx)
	}
	var out []reconcile.Request
	for _, req := range classesForConfigMap(r.Client, r.Config.ControllerName)(ctx, obj) {
		gc := &gwv1.GatewayClass{}
		gc.Name = req.Name
		out = append(out, r.gatewaysForClass(ctx, gc)...)
	}
	return out
}

// gatewaysForRoute maps a route to the Gateways in its parentRefs and in
// its status (so that detaching updates the old Gateway).
func (r *GatewayReconciler) gatewaysForRoute(ctx context.Context, obj client.Object) []reconcile.Request {
	var parents []gwv1.ParentReference
	var status *gwv1.RouteStatus
	switch rt := obj.(type) {
	case *gwv1.HTTPRoute:
		parents, status = rt.Spec.ParentRefs, &rt.Status.RouteStatus
	case *gwv1.GRPCRoute:
		parents, status = rt.Spec.ParentRefs, &rt.Status.RouteStatus
	case *gwv1.TLSRoute:
		parents, status = rt.Spec.ParentRefs, &rt.Status.RouteStatus
	case *gwv1.TCPRoute:
		parents, status = rt.Spec.ParentRefs, &rt.Status.RouteStatus
	case *gwv1.UDPRoute:
		parents, status = rt.Spec.ParentRefs, &rt.Status.RouteStatus
	default:
		return nil
	}
	for _, p := range status.Parents {
		if string(p.ControllerName) == r.Config.ControllerName {
			parents = append(parents, p.ParentRef)
		}
	}
	seen := map[types.NamespacedName]bool{}
	var out []reconcile.Request
	for _, p := range parents {
		if key, ok := listenerSetParentRef(p, obj.GetNamespace()); ok {
			// A route of a ListenerSet belongs to the ListenerSet's
			// Gateway. A ListenerSet that is gone leaves stale status,
			// which any Gateway's reconcile removes.
			if !r.Features.ListenerSet {
				continue
			}
			ls := &gwv1.ListenerSet{}
			var reqs []reconcile.Request
			if err := r.Get(ctx, key, ls); err == nil {
				reqs = r.gatewayForListenerSet(ctx, ls)
			} else if apierrors.IsNotFound(err) {
				reqs = firstGateway(r.managedGateways(ctx))
			}
			for _, req := range reqs {
				if !seen[req.NamespacedName] {
					seen[req.NamespacedName] = true
					out = append(out, req)
				}
			}
			continue
		}
		if p.Group != nil && string(*p.Group) != gwv1.GroupName || p.Kind != nil && string(*p.Kind) != translate.KindGateway {
			continue
		}
		ns := obj.GetNamespace()
		if p.Namespace != nil {
			ns = string(*p.Namespace)
		}
		key := types.NamespacedName{Namespace: ns, Name: string(p.Name)}
		if !seen[key] {
			seen[key] = true
			out = append(out, reconcile.Request{NamespacedName: key})
		}
	}
	return out
}

func firstGateway(reqs []reconcile.Request) []reconcile.Request {
	if len(reqs) > 1 {
		return reqs[:1]
	}
	return reqs
}

// gatewayForListenerSet maps a ListenerSet to the Gateway it names.
func (r *GatewayReconciler) gatewayForListenerSet(_ context.Context, obj client.Object) []reconcile.Request {
	ls, ok := obj.(*gwv1.ListenerSet)
	if !ok {
		return nil
	}
	key, ok := listenerSetGateway(ls)
	if !ok {
		return nil
	}
	return []reconcile.Request{{NamespacedName: key}}
}

// writeListenerSetStatus writes the status of the ListenerSets that name
// gw, once the Gateway's own status is written: Programmed follows the
// Gateway's.
func (r *GatewayReconciler) writeListenerSetStatus(ctx context.Context, gw *gwv1.Gateway, sets []*gwv1.ListenerSet, res *translate.Result, ip *invalidParamsError) error {
	results := map[types.NamespacedName]translate.ListenerSetResult{}
	for _, lr := range res.ListenerSets {
		results[lr.Ref] = lr
	}
	gp := meta.FindStatusCondition(gw.Status.Conditions, string(gwv1.GatewayConditionProgrammed))
	var errs []error
	for _, ls := range sets {
		lr, ok := results[types.NamespacedName{Namespace: ls.Namespace, Name: ls.Name}]
		if !ok {
			continue
		}
		gen := ls.Generation
		accepted, reason, msg := lr.Accepted, lr.Reason, lr.Message
		if ip != nil && reason != string(gwv1.ListenerSetReasonNotAllowed) {
			accepted, reason = false, string(gwv1.ListenerSetReasonParentNotAccepted)
			msg = fmt.Sprintf("Gateway %s/%s is not accepted: GatewayClass parameters are invalid", gw.Namespace, gw.Name)
		}
		ac := metav1.Condition{Type: string(gwv1.ListenerSetConditionAccepted), Status: metav1.ConditionTrue,
			Reason: reason, Message: msg, ObservedGeneration: gen}
		pc := metav1.Condition{Type: string(gwv1.ListenerSetConditionProgrammed), Status: metav1.ConditionFalse,
			Reason: reason, Message: msg, ObservedGeneration: gen}
		switch {
		case !accepted:
			ac.Status = metav1.ConditionFalse
			if reason == string(gwv1.ListenerSetReasonParentNotAccepted) {
				pc.Reason = reasonParentNotProgrammed
			}
		case gp != nil && gp.Status == metav1.ConditionTrue:
			pc.Status, pc.Reason, pc.Message = metav1.ConditionTrue, string(gwv1.ListenerSetReasonProgrammed), gp.Message
		default:
			pc.Reason = reasonParentNotProgrammed
			pc.Message = "the parent Gateway is not programmed"
			if gp != nil {
				pc.Message += ": " + gp.Message
			}
		}
		// Listener Programmed follows the ListenerSet's.
		lp := metav1.Condition{Status: pc.Status, Message: pc.Message}
		orig := ls.DeepCopy()
		meta.SetStatusCondition(&ls.Status.Conditions, ac)
		meta.SetStatusCondition(&ls.Status.Conditions, pc)
		var listeners []gwv1.ListenerEntryStatus
		for _, l := range lr.Listeners {
			var prev []metav1.Condition
			for _, old := range orig.Status.Listeners {
				if old.Name == l.Name {
					prev = old.Conditions
				}
			}
			conds, kinds := listenerConditions(prev, l, lp, gen)
			listeners = append(listeners, gwv1.ListenerEntryStatus{Name: l.Name, SupportedKinds: kinds, AttachedRoutes: l.AttachedRoutes, Conditions: conds})
		}
		ls.Status.Listeners = listeners
		if listenerSetStatusEqual(&orig.Status, &ls.Status) {
			continue
		}
		if err := r.Status().Patch(ctx, ls, client.MergeFrom(orig)); client.IgnoreNotFound(err) != nil {
			errs = append(errs, fmt.Errorf("ListenerSet %s/%s status: %w", ls.Namespace, ls.Name, err))
		}
	}
	return errors.Join(errs...)
}

// reasonParentNotProgrammed is the ListenerSet Programmed reason the
// Gateway API documents for a parent Gateway that is not programmed.
const reasonParentNotProgrammed = "ParentNotProgrammed"

// releaseListenerSets marks the ListenerSets that name gw as not accepted
// when gw stops serving them for good (deletion) or for now (its class is
// gone).
func (r *GatewayReconciler) releaseListenerSets(ctx context.Context, gw *gwv1.Gateway, why string) error {
	sets, err := r.listListenerSets(ctx)
	if err != nil {
		return err
	}
	var errs []error
	for _, ls := range sets.of(gw) {
		errs = append(errs, r.releaseListenerSet(ctx, ls, metav1.ConditionFalse,
			string(gwv1.ListenerSetReasonParentNotAccepted), reasonParentNotProgrammed, why))
	}
	return errors.Join(errs...)
}

// releaseListenerSet sets a ListenerSet's Accepted and Programmed
// conditions and drops its listener status.
func (r *GatewayReconciler) releaseListenerSet(ctx context.Context, ls *gwv1.ListenerSet, status metav1.ConditionStatus, acceptedReason, programmedReason, why string) error {
	orig := ls.DeepCopy()
	for _, c := range []metav1.Condition{
		{Type: string(gwv1.ListenerSetConditionAccepted), Reason: acceptedReason},
		{Type: string(gwv1.ListenerSetConditionProgrammed), Reason: programmedReason},
	} {
		c.Status, c.Message, c.ObservedGeneration = status, why, ls.Generation
		meta.SetStatusCondition(&ls.Status.Conditions, c)
	}
	ls.Status.Listeners = nil
	if listenerSetStatusEqual(&orig.Status, &ls.Status) {
		return nil
	}
	return client.IgnoreNotFound(r.Status().Patch(ctx, ls, client.MergeFrom(orig)))
}

// releaseMovedListenerSets hands back ListenerSets whose status gw wrote
// but whose parentRef now names another Gateway that no Gateway of this
// controller serves: one that does not exist gets ParentNotAccepted,
// another controller's gets the "waiting for controller" conditions the
// CRD starts with. Status that is already about the ListenerSet's current
// generation was written after the move, by whoever serves it now, and is
// left alone. The ListenerSets gw serves are then recorded on gw.
func (r *GatewayReconciler) releaseMovedListenerSets(ctx context.Context, gw *gwv1.Gateway, sets listenerSets) error {
	var current []string
	for _, ls := range sets.of(gw) {
		current = append(current, ls.Namespace+"/"+ls.Name)
	}
	var errs []error
	for _, k := range strings.Split(gw.Annotations[config.AnnotationListenerSets], ",") {
		ns, name, ok := strings.Cut(k, "/")
		if !ok || slices.Contains(current, k) {
			continue
		}
		ls := sets[types.NamespacedName{Namespace: ns, Name: name}]
		if ls == nil {
			continue // deleted
		}
		if c := meta.FindStatusCondition(ls.Status.Conditions, string(gwv1.ListenerSetConditionAccepted)); c == nil || c.ObservedGeneration >= ls.Generation {
			continue
		}
		parent, ok := listenerSetGateway(ls)
		if !ok {
			continue
		}
		other := &gwv1.Gateway{}
		switch err := r.Get(ctx, parent, other); {
		case apierrors.IsNotFound(err):
			errs = append(errs, r.releaseListenerSet(ctx, ls, metav1.ConditionFalse, string(gwv1.ListenerSetReasonParentNotAccepted),
				reasonParentNotProgrammed, fmt.Sprintf("Gateway %s not found", parent)))
		case err != nil:
			errs = append(errs, err)
		default:
			gc, err := r.managedClass(ctx, string(other.Spec.GatewayClassName))
			switch {
			case err != nil:
				errs = append(errs, err)
			case gc == nil:
				errs = append(errs, r.releaseListenerSet(ctx, ls, metav1.ConditionUnknown, string(gwv1.ListenerSetReasonPending),
					string(gwv1.ListenerSetReasonPending), "Waiting for controller"))
			}
		}
	}
	if err := errors.Join(errs...); err != nil {
		return err // keep the record and retry
	}
	value := strings.Join(current, ",")
	if gw.Annotations[config.AnnotationListenerSets] == value {
		return nil
	}
	orig := gw.DeepCopy()
	if value == "" {
		delete(gw.Annotations, config.AnnotationListenerSets)
	} else {
		if gw.Annotations == nil {
			gw.Annotations = map[string]string{}
		}
		gw.Annotations[config.AnnotationListenerSets] = value
	}
	return r.Patch(ctx, gw, client.MergeFromWithOptions(orig, client.MergeFromWithOptimisticLock{}))
}

func listenerSetStatusEqual(a, b *gwv1.ListenerSetStatus) bool {
	if !equalConditions(a.Conditions, b.Conditions) || len(a.Listeners) != len(b.Listeners) {
		return false
	}
	for i := range a.Listeners {
		x, y := a.Listeners[i], b.Listeners[i]
		if x.Name != y.Name || x.AttachedRoutes != y.AttachedRoutes || len(x.SupportedKinds) != len(y.SupportedKinds) || !equalConditions(x.Conditions, y.Conditions) {
			return false
		}
		for j := range x.SupportedKinds {
			if x.SupportedKinds[j].Kind != y.SupportedKinds[j].Kind {
				return false
			}
		}
	}
	return true
}

// SetupWithManager registers the reconciler and its watches.
func (r *GatewayReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.APIReader = mgr.GetAPIReader()
	routeHandler := handler.EnqueueRequestsFromMapFunc(r.gatewaysForRoute)
	createDelete := predicate.Funcs{
		CreateFunc:  func(event.CreateEvent) bool { return true },
		DeleteFunc:  func(event.DeleteEvent) bool { return true },
		UpdateFunc:  func(event.UpdateEvent) bool { return false },
		GenericFunc: func(event.GenericEvent) bool { return false },
	}
	b := ctrl.NewControllerManagedBy(mgr).
		Named("gateway").
		For(&gwv1.Gateway{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&corev1.Secret{}).
		Watches(&gwv1.GatewayClass{}, handler.EnqueueRequestsFromMapFunc(r.gatewaysForClass)).
		Watches(&corev1.ConfigMap{}, handler.EnqueueRequestsFromMapFunc(r.gatewaysForParamsConfigMap)).
		Watches(&corev1.Service{}, handler.EnqueueRequestsFromMapFunc(r.allGateways), builder.WithPredicates(createDelete)).
		Watches(&corev1.Namespace{}, handler.EnqueueRequestsFromMapFunc(r.allGateways), builder.WithPredicates(predicate.LabelChangedPredicate{})).
		Watches(&gwv1.HTTPRoute{}, routeHandler)
	if r.Features.GRPCRoute {
		b = b.Watches(&gwv1.GRPCRoute{}, routeHandler)
	}
	if r.Features.TLSRoute {
		b = b.Watches(&gwv1.TLSRoute{}, routeHandler)
	}
	if r.Features.TCPRoute {
		b = b.Watches(&gwv1.TCPRoute{}, routeHandler)
	}
	if r.Features.UDPRoute {
		b = b.Watches(&gwv1.UDPRoute{}, routeHandler)
	}
	if r.Features.ReferenceGrant {
		b = b.Watches(&gwv1.ReferenceGrant{}, handler.EnqueueRequestsFromMapFunc(r.allGateways))
	}
	if r.Features.BackendTLSPolicy {
		b = b.Watches(&gwv1.BackendTLSPolicy{}, handler.EnqueueRequestsFromMapFunc(r.allGateways))
	}
	if r.Features.ListenerSet {
		// Old and new parents are both enqueued on update. Status writes
		// do not change the generation and are skipped.
		b = b.Watches(&gwv1.ListenerSet{}, handler.EnqueueRequestsFromMapFunc(r.gatewayForListenerSet),
			builder.WithPredicates(predicate.GenerationChangedPredicate{}))
	}
	return b.Complete(r)
}
