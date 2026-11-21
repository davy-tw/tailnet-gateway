package controller

import (
	"context"
	"testing"

	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"go.pkgs.davy.tw/tailnet-gateway/internal/config"
)

func TestIngressLegacyClassAnnotation(t *testing.T) {
	cfg := testConfig(t)
	ic := &networkingv1.IngressClass{
		ObjectMeta: metav1.ObjectMeta{Name: cfg.IngressClass, Annotations: map[string]string{annotationDefaultIngressClass: "true"}},
		Spec:       networkingv1.IngressClassSpec{Controller: config.IngressControllerName},
	}
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(ic).Build()
	r := NewIngressReconciler(c, c.Scheme(), cfg, nil)

	for _, tc := range []struct {
		name   string
		class  *string
		legacy *string
		want   bool
	}{
		{"default class", nil, nil, true},
		{"named", ptr.To(cfg.IngressClass), nil, true},
		{"other class", ptr.To("nginx"), nil, false},
		{"legacy other class", nil, ptr.To("nginx"), false},
		{"legacy this class", nil, ptr.To(cfg.IngressClass), true},
		{"field wins over legacy", ptr.To(cfg.IngressClass), ptr.To("nginx"), true},
	} {
		ing := &networkingv1.Ingress{ObjectMeta: metav1.ObjectMeta{Namespace: "a", Name: "x"}}
		ing.Spec.IngressClassName = tc.class
		if tc.legacy != nil {
			ing.Annotations = map[string]string{annotationLegacyIngressClass: *tc.legacy}
		}
		got, err := r.managed(context.Background(), ing)
		if err != nil || got != tc.want {
			t.Errorf("%s: managed = %v, %v; want %v", tc.name, got, err, tc.want)
		}
	}
}
