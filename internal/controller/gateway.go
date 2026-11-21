package controller

import (
	"context"
	"errors"
	"fmt"
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
	gc, err := r.managedClass(ctx, string(gw.Spec.GatewayClassName))
	if err != nil {
		return ctrl.Result{}, err
	}
	if !gw.DeletionTimestamp.IsZero() || gc == nil {
		if !controllerutil.ContainsFinalizer(gw, config.Finalizer) {
			return ctrl.Result{}, nil
		}
		if err := r.finalize(ctx, gw, gc == nil && gw.DeletionTimestamp.IsZero()); err != nil {
			return ctrl.Result{}, err
		}
		orig := gw.DeepCopy()
		controllerutil.RemoveFinalizer(gw, config.Finalizer)
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
	in, err := r.translateInput(ctx, gw, routes)
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
	res := translate.Translate(in)

	// Route and policy status are written whatever happens next.
	statusErr := errors.Join(
		r.writeRouteStatus(ctx, gw, routes, res),
		r.writePolicyStatus(ctx, gw, res),
	)

	var progErr error
	var sts *appsv1.StatefulSet
	switch {
	case ip != nil:
		progErr = &pendingError{"GatewayClass parameters are invalid: " + ip.msg}
	case !res.Accepted:
		progErr = &pendingError{"Gateway is not accepted: " + res.AcceptedMessage}
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
	if statusErr != nil {
		return ctrl.Result{}, statusErr
	}
	if progErr != nil && pe == nil {
		return ctrl.Result{}, progErr
	}
	switch {
	case ip != nil || !res.Accepted:
		return ctrl.Result{}, nil // a spec or class change will trigger the next reconcile
	case !programmed || res.WebPending:
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

func (r *GatewayReconciler) translateInput(ctx context.Context, gw *gwv1.Gateway, rs *routeSet) (*translate.Input, error) {
	in := &translate.Input{
		Gateway:        gw,
		ControllerName: r.Config.ControllerName,
		HTTPRoutes:     rs.http, GRPCRoutes: rs.grpc, TLSRoutes: rs.tls, TCPRoutes: rs.tcp, UDPRoutes: rs.udp,
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
	if needsNamespaceLabels(gw) {
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

func needsNamespaceLabels(gw *gwv1.Gateway) bool {
	for _, l := range gw.Spec.Listeners {
		if l.AllowedRoutes != nil && l.AllowedRoutes.Namespaces != nil && l.AllowedRoutes.Namespaces.From != nil &&
			*l.AllowedRoutes.Namespaces.From == gwv1.NamespacesFromSelector {
			return true
		}
	}
	return false
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
	spec := &proxy.Spec{
		Namespace: gw.Namespace,
		Gateway:   gw.Name,
		Params:    params,
		PodLabels: gw.Labels,
		Owner:     ownerRef(gw),
	}
	cfg, err := res.Config.Marshal()
	if err != nil {
		return nil, err
	}
	for _, obj := range []client.Object{spec.ServiceAccount(), spec.Role(), spec.RoleBinding(), spec.ConfigMap(cfg)} {
		if err := apply(ctx, r.Client, r.Scheme, obj); err != nil {
			return nil, fmt.Errorf("applying %T %s: %w", obj, obj.GetName(), err)
		}
	}
	if pdb := spec.PodDisruptionBudget(); pdb != nil {
		if err := apply(ctx, r.Client, r.Scheme, pdb); err != nil {
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
	if err := apply(ctx, r.Client, r.Scheme, spec.StatefulSet()); err != nil {
		return nil, fmt.Errorf("applying StatefulSet: %w", err)
	}
	if err := r.pruneStateSecrets(ctx, gw, params.Replicas); err != nil {
		return nil, err
	}
	return r.currentStatefulSet(ctx, gw), nil
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
	user := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: gw.Namespace, Name: gw.Name}, user); err != nil {
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
		err := r.Get(ctx, types.NamespacedName{Namespace: gw.Namespace, Name: name}, sec)
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
	if err != nil || exists {
		return err
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
		if err := r.deleteDevice(ctx, gw, s); err != nil {
			return err
		}
		if err := r.Delete(ctx, s); client.IgnoreNotFound(err) != nil {
			return err
		}
	}
	return nil
}

func (r *GatewayReconciler) deleteDevice(ctx context.Context, gw *gwv1.Gateway, s *corev1.Secret) error {
	id := string(s.Data[proxy.KeyDeviceID])
	if id == "" {
		return nil
	}
	if r.TS == nil {
		log.FromContext(ctx).Info("manual key mode: not deleting device; ephemeral devices expire on their own", "device", id, "secret", s.Name)
		return nil
	}
	if err := r.TS.DeleteDevice(ctx, id); err != nil {
		return fmt.Errorf("deleting device %s: %w", id, err)
	}
	return nil
}

// finalize deletes the Gateway's devices. When the Gateway merely stopped
// being managed (unmanage), it also removes the data plane and statuses.
func (r *GatewayReconciler) finalize(ctx context.Context, gw *gwv1.Gateway, unmanage bool) error {
	secrets, err := r.stateSecrets(ctx, gw)
	if err != nil {
		return err
	}
	if r.TS == nil && len(secrets) > 0 {
		r.event(gw, corev1.EventTypeWarning, "DevicesNotDeleted", "no OAuth client configured: devices are not deleted (ephemeral devices expire on their own)")
	}
	for i := range secrets {
		if err := r.deleteDevice(ctx, gw, &secrets[i]); err != nil {
			return err
		}
	}
	routes, err := r.listRoutes(ctx)
	if err != nil {
		return err
	}
	if err := r.writeRouteStatus(ctx, gw, routes, &translate.Result{}); err != nil {
		return err
	}
	if err := r.writePolicyStatus(ctx, gw, &translate.Result{}); err != nil {
		return err
	}
	if !unmanage {
		return nil // owner references garbage-collect the rest
	}
	for _, o := range []client.Object{&appsv1.StatefulSet{}, &policyv1.PodDisruptionBudget{}, &corev1.ConfigMap{},
		&rbacv1.RoleBinding{}, &rbacv1.Role{}, &corev1.ServiceAccount{}} {
		if err := r.deleteOwned(ctx, gw, o, gw.Name); err != nil {
			return err
		}
	}
	for i := range secrets {
		if err := r.Delete(ctx, &secrets[i]); client.IgnoreNotFound(err) != nil {
			return err
		}
	}
	return nil
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
	case progErr != nil:
		programmed.Message = fmt.Sprintf("%v (%s)", progErr, counts)
	case ready == 0:
		programmed.Message = "waiting for a proxy replica to become ready (" + counts + ")"
	case res.WebPending:
		programmed.Message = "waiting to learn the tailnet domain from a proxy replica (" + counts + ")"
	default:
		programmed.Status = metav1.ConditionTrue
		programmed.Reason = string(gwv1.GatewayReasonProgrammed)
		programmed.Message = counts
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
		conds := prev
		var keep []metav1.Condition
		for _, c := range conds {
			if meta.FindStatusCondition(lr.Conditions, c.Type) != nil || c.Type == string(gwv1.ListenerConditionProgrammed) {
				keep = append(keep, c)
			}
		}
		conds = keep
		for _, c := range lr.Conditions {
			meta.SetStatusCondition(&conds, c)
		}
		lp := metav1.Condition{Type: string(gwv1.ListenerConditionProgrammed), ObservedGeneration: gen,
			Status: programmed.Status, Reason: string(gwv1.ListenerReasonProgrammed), Message: programmed.Message}
		if !lr.Valid {
			lp.Status, lp.Reason, lp.Message = metav1.ConditionFalse, string(gwv1.ListenerReasonInvalid), "listener is not valid"
		} else if programmed.Status != metav1.ConditionTrue {
			lp.Reason = string(gwv1.ListenerReasonPending)
		}
		meta.SetStatusCondition(&conds, lp)
		kinds := lr.SupportedKinds
		if kinds == nil {
			kinds = []gwv1.RouteGroupKind{}
		}
		listeners = append(listeners, gwv1.ListenerStatus{Name: lr.Name, SupportedKinds: kinds, AttachedRoutes: lr.AttachedRoutes, Conditions: conds})
	}
	st.Listeners = listeners

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

func gatewayStatusEqual(a, b *gwv1.GatewayStatus) bool {
	if !equalConditions(a.Conditions, b.Conditions) || len(a.Listeners) != len(b.Listeners) || len(a.Addresses) != len(b.Addresses) {
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
func (r *GatewayReconciler) writeRouteStatus(ctx context.Context, gw *gwv1.Gateway, rs *routeSet, res *translate.Result) error {
	results := map[translate.RouteRef]translate.RouteResult{}
	for _, rr := range res.Routes {
		results[rr.Ref] = rr
	}
	var errs []error
	for _, t := range rs.targets() {
		ref := translate.RouteRef{Kind: t.kind, Namespace: t.obj.GetNamespace(), Name: t.obj.GetName()}
		result, has := results[ref]
		ours := func(p gwv1.RouteParentStatus) bool {
			return string(p.ControllerName) == r.Config.ControllerName && parentRefTargets(p.ParentRef, ref.Namespace, gw.Namespace, gw.Name)
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

// gatewayForUserSecret maps a user's auth Secret to the Gateway of the same
// name.
func (r *GatewayReconciler) gatewayForUserSecret(_ context.Context, obj client.Object) []reconcile.Request {
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: obj.GetNamespace(), Name: obj.GetName()}}}
}

// SetupWithManager registers the reconciler and its watches.
func (r *GatewayReconciler) SetupWithManager(mgr ctrl.Manager) error {
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
		Watches(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(r.gatewayForUserSecret),
			builder.WithPredicates(predicate.NewPredicateFuncs(func(o client.Object) bool {
				return o.GetLabels()[proxy.LabelManagedBy] != proxy.ManagedBy
			}))).
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
	return b.Complete(r)
}
