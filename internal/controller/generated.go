package controller

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"

	"go.pkgs.davy.tw/tailnet-gateway/internal/config"
)

// generator holds what the Ingress and Service translators share: they
// write Gateways and routes owned by a source object.
type generator struct {
	classResolver
	Scheme *runtime.Scheme
}

func sourceMeta(owner client.Object, gvkKind, apiVersion, source, name string) metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Name:        name,
		Namespace:   owner.GetNamespace(),
		Annotations: map[string]string{config.AnnotationSource: source},
		Labels:      map[string]string{config.LabelSourceKind: source, config.LabelSourceName: owner.GetName()},
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: apiVersion, Kind: gvkKind, Name: owner.GetName(), UID: owner.GetUID(),
			Controller: ptr.To(true), BlockOwnerDeletion: ptr.To(true),
		}},
	}
}

// conflictError is a generated object name already used by something else.
type conflictError struct{ msg string }

func (e *conflictError) Error() string { return e.msg }

// applyGenerated applies obj unless an object with that name exists and is
// not controlled by owner.
func (g *generator) applyGenerated(ctx context.Context, owner client.Object, obj client.Object) error {
	existing := obj.DeepCopyObject().(client.Object)
	err := g.Get(ctx, types.NamespacedName{Namespace: obj.GetNamespace(), Name: obj.GetName()}, existing)
	switch {
	case apierrors.IsNotFound(err):
	case err != nil:
		return err
	case !metav1.IsControlledBy(existing, owner):
		return &conflictError{fmt.Sprintf("%T %s/%s already exists and is not managed by this object", obj, obj.GetNamespace(), obj.GetName())}
	}
	return apply(ctx, g.Client, g.Scheme, obj)
}

// pruneGenerated deletes objects of list's kind generated for owner whose
// names are not in keep.
func (g *generator) pruneGenerated(ctx context.Context, owner client.Object, source string, list client.ObjectList, keep map[string]bool) error {
	if err := g.List(ctx, list, client.InNamespace(owner.GetNamespace()),
		client.MatchingLabels{config.LabelSourceKind: source, config.LabelSourceName: owner.GetName()}); err != nil {
		if apierrors.IsNotFound(err) || isNoKindMatch(err) {
			return nil
		}
		return err
	}
	items, err := listItems(list)
	if err != nil {
		return err
	}
	for _, o := range items {
		if keep[o.GetName()] || !metav1.IsControlledBy(o, owner) {
			continue
		}
		if err := g.Delete(ctx, o); client.IgnoreNotFound(err) != nil {
			return err
		}
	}
	return nil
}

func listItems(list client.ObjectList) ([]client.Object, error) {
	var out []client.Object
	switch l := list.(type) {
	case *gwv1.GatewayList:
		for i := range l.Items {
			out = append(out, &l.Items[i])
		}
	case *gwv1.HTTPRouteList:
		for i := range l.Items {
			out = append(out, &l.Items[i])
		}
	case *gwv1.TCPRouteList:
		for i := range l.Items {
			out = append(out, &l.Items[i])
		}
	default:
		return nil, fmt.Errorf("unsupported list %T", list)
	}
	return out, nil
}

func isNoKindMatch(err error) bool { return meta.IsNoMatchError(err) }

// gatewayHostnames returns the Hostname addresses of a generated Gateway,
// or nil while it is not programmed.
func (g *generator) gatewayHostnames(ctx context.Context, ns, name string) []string {
	gw := &gwv1.Gateway{}
	if err := g.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, gw); err != nil {
		return nil
	}
	var out []string
	for _, a := range gw.Status.Addresses {
		if a.Type != nil && *a.Type == gwv1.HostnameAddressType {
			out = append(out, a.Value)
		}
	}
	return out
}

// tailscaleTLSOptions satisfies the Gateway API rule that a terminating
// listener names certificates or options: certificates come from Tailscale.
func tailscaleTLSOptions() *gwv1.ListenerTLSConfig {
	return &gwv1.ListenerTLSConfig{
		Mode:    ptr.To(gwv1.TLSModeTerminate),
		Options: map[gwv1.AnnotationKey]gwv1.AnnotationValue{gwv1.AnnotationKey(config.Domain + "/certificates"): "tailscale"},
	}
}
