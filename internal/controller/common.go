// Package controller holds the reconcilers.
package controller

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/davy-tw/tailnet-gateway/internal/translate"
)

// FieldOwner is the server-side apply field manager.
const FieldOwner = "tailnet-gateway"

// Features records which optional Gateway API kinds the cluster serves.
type Features struct {
	GRPCRoute        bool
	TLSRoute         bool
	TCPRoute         bool
	UDPRoute         bool
	ReferenceGrant   bool
	BackendTLSPolicy bool
}

// SupportedRouteKinds is the translate.Input.Supported map.
func (f Features) SupportedRouteKinds() map[string]bool {
	return map[string]bool{
		translate.KindHTTPRoute: true,
		translate.KindGRPCRoute: f.GRPCRoute,
		translate.KindTLSRoute:  f.TLSRoute,
		translate.KindTCPRoute:  f.TCPRoute,
		translate.KindUDPRoute:  f.UDPRoute,
	}
}

// AllFeatures enables every optional kind (for tests).
func AllFeatures() Features {
	return Features{GRPCRoute: true, TLSRoute: true, TCPRoute: true, UDPRoute: true, ReferenceGrant: true, BackendTLSPolicy: true}
}

// ErrMissingCoreCRDs is returned by DetectFeatures when GatewayClass,
// Gateway or HTTPRoute is not served at v1.
type ErrMissingCoreCRDs struct{ Missing []string }

func (e *ErrMissingCoreCRDs) Error() string {
	return fmt.Sprintf("Gateway API v1.6.0 or newer (Standard channel) is required; %s not served at %s/v1", e.Missing, gwv1.GroupName)
}

// DetectFeatures checks which Gateway API kinds are served at v1.
func DetectFeatures(dc discovery.DiscoveryInterface) (Features, error) {
	kinds := map[string]bool{}
	list, err := dc.ServerResourcesForGroupVersion(gwv1.GroupVersion.String())
	if err != nil && !apierrors.IsNotFound(err) {
		return Features{}, err
	}
	if list != nil {
		for _, r := range list.APIResources {
			kinds[r.Kind] = true
		}
	}
	var missing []string
	for _, k := range []string{"GatewayClass", "Gateway", "HTTPRoute"} {
		if !kinds[k] {
			missing = append(missing, k)
		}
	}
	if len(missing) > 0 {
		return Features{}, &ErrMissingCoreCRDs{Missing: missing}
	}
	return Features{
		GRPCRoute:        kinds["GRPCRoute"],
		TLSRoute:         kinds["TLSRoute"],
		TCPRoute:         kinds["TCPRoute"],
		UDPRoute:         kinds["UDPRoute"],
		ReferenceGrant:   kinds["ReferenceGrant"],
		BackendTLSPolicy: kinds["BackendTLSPolicy"],
	}, nil
}

// apply server-side applies obj, which must be a fully specified typed
// object, as FieldOwner.
func apply(ctx context.Context, c client.Client, scheme *runtime.Scheme, obj client.Object) error {
	gvk, err := apiutil.GVKForObject(obj, scheme)
	if err != nil {
		return err
	}
	return applyWithGVK(ctx, c, obj, gvk)
}

func applyWithGVK(ctx context.Context, c client.Client, obj client.Object, gvk schema.GroupVersionKind) error {
	m, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		return err
	}
	u := &unstructured.Unstructured{Object: m}
	u.SetGroupVersionKind(gvk)
	delete(u.Object, "status")
	unstructured.RemoveNestedField(u.Object, "metadata", "creationTimestamp")
	unstructured.RemoveNestedField(u.Object, "spec", "template", "metadata", "creationTimestamp")
	return c.Apply(ctx, client.ApplyConfigurationFromUnstructured(u), client.FieldOwner(FieldOwner), client.ForceOwnership)
}

// gatewayParentRef is the parentRef/ancestorRef form of a Gateway.
func gatewayParentRef(gw *gwv1.Gateway) gwv1.ParentReference {
	g := gwv1.Group(gwv1.GroupName)
	k := gwv1.Kind(translate.KindGateway)
	ns := gwv1.Namespace(gw.Namespace)
	return gwv1.ParentReference{Group: &g, Kind: &k, Namespace: &ns, Name: gwv1.ObjectName(gw.Name)}
}

// parentRefTargets reports whether p (from an object in routeNS) points at
// the Gateway ns/name.
func parentRefTargets(p gwv1.ParentReference, routeNS, ns, name string) bool {
	if p.Group != nil && string(*p.Group) != gwv1.GroupName {
		return false
	}
	if p.Kind != nil && string(*p.Kind) != translate.KindGateway {
		return false
	}
	pns := routeNS
	if p.Namespace != nil {
		pns = string(*p.Namespace)
	}
	return pns == ns && string(p.Name) == name
}
