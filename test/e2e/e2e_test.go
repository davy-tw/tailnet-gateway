//go:build e2e

// Package e2e runs against a real cluster (the current kubeconfig context)
// with the Gateway API CRDs and tailnet-gateway installed:
//
//	go test -tags e2e ./test/e2e/ -v
//
// With E2E_EXPECT_PROGRAMMED=1 (a tailnet with OAuth or an auth key, and
// the Tailscale Service svc:e2e-<suffix> approved), it also waits for the
// Gateway to be Programmed and to publish an address.
package e2e

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"
)

func TestGatewaySmoke(t *testing.T) {
	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(gwv1.Install(scheme))
	c, err := client.New(ctrl.GetConfigOrDie(), client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	class := os.Getenv("E2E_GATEWAY_CLASS")
	if class == "" {
		class = "tailnet"
	}
	ns := fmt.Sprintf("tailnet-e2e-%d", time.Now().Unix())
	if err := c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = c.Delete(context.Background(), &gwv1.Gateway{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "e2e"}})
		_ = c.Delete(context.Background(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}})
	})
	svcLabel := ns // unique Tailscale Service name per run
	objs := []client.Object{
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "web"},
			Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Name: "http", Port: 80}}}},
		&gwv1.Gateway{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "e2e"},
			Spec: gwv1.GatewaySpec{GatewayClassName: gwv1.ObjectName(class),
				Listeners: []gwv1.Listener{{Name: "http", Protocol: gwv1.HTTPProtocolType, Port: 80}}}},
		&gwv1.HTTPRoute{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "e2e"},
			Spec: gwv1.HTTPRouteSpec{
				CommonRouteSpec: gwv1.CommonRouteSpec{ParentRefs: []gwv1.ParentReference{{Name: "e2e"}}},
				Hostnames:       []gwv1.Hostname{gwv1.Hostname(svcLabel)},
				Rules: []gwv1.HTTPRouteRule{{BackendRefs: []gwv1.HTTPBackendRef{{BackendRef: gwv1.BackendRef{
					BackendObjectReference: gwv1.BackendObjectReference{Name: "web", Port: ptr.To(gwv1.PortNumber(80))}}}}}},
			}},
	}
	for _, o := range objs {
		if err := c.Create(ctx, o); err != nil {
			t.Fatal(err)
		}
	}

	wait := func(what string, timeout time.Duration, f func() error) {
		t.Helper()
		deadline := time.Now().Add(timeout)
		var err error
		for time.Now().Before(deadline) {
			if err = f(); err == nil {
				return
			}
			time.Sleep(2 * time.Second)
		}
		t.Fatalf("%s: %v", what, err)
	}
	condTrue := func(conds []metav1.Condition, typ string) error {
		cnd := meta.FindStatusCondition(conds, typ)
		if cnd == nil || cnd.Status != metav1.ConditionTrue {
			return fmt.Errorf("%s not true: %+v", typ, cnd)
		}
		return nil
	}
	key := types.NamespacedName{Namespace: ns, Name: "e2e"}
	wait("Gateway accepted", time.Minute, func() error {
		gw := &gwv1.Gateway{}
		if err := c.Get(ctx, key, gw); err != nil {
			return err
		}
		return condTrue(gw.Status.Conditions, "Accepted")
	})
	wait("HTTPRoute accepted", time.Minute, func() error {
		r := &gwv1.HTTPRoute{}
		if err := c.Get(ctx, key, r); err != nil {
			return err
		}
		if len(r.Status.Parents) == 0 {
			return fmt.Errorf("no parent status")
		}
		return condTrue(r.Status.Parents[0].Conditions, "Accepted")
	})
	wait("StatefulSet created", time.Minute, func() error {
		return c.Get(ctx, key, &appsv1.StatefulSet{})
	})
	if os.Getenv("E2E_EXPECT_PROGRAMMED") != "1" {
		return
	}
	wait("Gateway programmed", 5*time.Minute, func() error {
		gw := &gwv1.Gateway{}
		if err := c.Get(ctx, key, gw); err != nil {
			return err
		}
		if err := condTrue(gw.Status.Conditions, "Programmed"); err != nil {
			return err
		}
		if len(gw.Status.Addresses) == 0 {
			return fmt.Errorf("no addresses")
		}
		t.Logf("Gateway address: %s", gw.Status.Addresses[0].Value)
		return nil
	})
}
