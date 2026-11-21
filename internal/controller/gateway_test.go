package controller

import (
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"

	"go.pkgs.davy.tw/tailnet-gateway/internal/config"
	"go.pkgs.davy.tw/tailnet-gateway/internal/translate"
)

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := gwv1.Install(s); err != nil {
		t.Fatal(err)
	}
	return s
}

func testConfig(t *testing.T) *config.Config {
	t.Helper()
	cfg, err := config.Load(func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func TestIsServeConfig(t *testing.T) {
	cfg := testConfig(t)
	ours := &gwv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: "tailnet"}, Spec: gwv1.GatewayClassSpec{ControllerName: gwv1.GatewayController(cfg.ControllerName)}}
	theirs := &gwv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: "other"}, Spec: gwv1.GatewayClassSpec{ControllerName: "example.com/other"}}
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(ours, theirs).Build()
	r := NewGatewayReconciler(c, c.Scheme(), cfg, nil, AllFeatures(), nil)

	gw := func(class string) *gwv1.Gateway {
		return &gwv1.Gateway{ObjectMeta: metav1.ObjectMeta{Namespace: "a", Name: "gw"}, Spec: gwv1.GatewaySpec{GatewayClassName: gwv1.ObjectName(class)}}
	}
	cm := func(name, manager string) *corev1.ConfigMap {
		return &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "a", Name: name, ManagedFields: []metav1.ManagedFieldsEntry{
			{Manager: manager, Operation: metav1.ManagedFieldsOperationApply},
		}}}
	}
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		cm   *corev1.ConfigMap
		gw   *gwv1.Gateway
		want bool
	}{
		{"ours", cm("gw", FieldOwner), gw("tailnet"), true},
		{"other name", cm("forged", FieldOwner), gw("tailnet"), false},
		{"other class", cm("gw", FieldOwner), gw("other"), false},
		{"missing class", cm("gw", FieldOwner), gw("gone"), false},
		{"written by someone else", cm("gw", "kubectl"), gw("tailnet"), false},
	} {
		if got := r.isServeConfig(ctx, tc.cm, tc.gw); got != tc.want {
			t.Errorf("%s: isServeConfig = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestProgrammedKeptWhileServing(t *testing.T) {
	cfg := testConfig(t)
	gw := &gwv1.Gateway{ObjectMeta: metav1.ObjectMeta{Namespace: "a", Name: "gw"}}
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(gw).WithStatusSubresource(gw).Build()
	r := NewGatewayReconciler(c, c.Scheme(), cfg, nil, AllFeatures(), nil)
	res := &translate.Result{Accepted: true, AcceptedReason: "Accepted", Services: []string{"web"}}
	ctx := context.Background()
	get := func() *gwv1.Gateway {
		g := &gwv1.Gateway{}
		if err := c.Get(ctx, client.ObjectKeyFromObject(gw), g); err != nil {
			t.Fatal(err)
		}
		return g
	}

	// One replica still serves while a step fails: stay programmed.
	if ok, err := r.writeGatewayStatus(ctx, get(), res, nil, errors.New("rate limited"), 1, 2, "tail1234.ts.net"); err != nil || !ok {
		t.Fatalf("programmed = %v, %v", ok, err)
	}
	g := get()
	p := meta.FindStatusCondition(g.Status.Conditions, "Programmed")
	if p.Status != metav1.ConditionTrue || len(g.Status.Addresses) != 1 {
		t.Errorf("serving replica: programmed = %+v, addresses = %+v", p, g.Status.Addresses)
	}

	// No replica is ready: not programmed, no addresses.
	if ok, err := r.writeGatewayStatus(ctx, g, res, nil, errors.New("rate limited"), 0, 2, "tail1234.ts.net"); err != nil || ok {
		t.Fatalf("programmed = %v, %v", ok, err)
	}
	g = get()
	p = meta.FindStatusCondition(g.Status.Conditions, "Programmed")
	if p.Status != metav1.ConditionFalse || len(g.Status.Addresses) != 0 {
		t.Errorf("no replica: programmed = %+v, addresses = %+v", p, g.Status.Addresses)
	}
}
