package controller

import (
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"

	"go.pkgs.davy.tw/tailnet-gateway/internal/proxy"
)

// TestEnvtestCertificateSharing covers the certificate Secrets: created
// empty per HTTPS name, granted to the proxies, a renewal (but not the
// first certificate) rolling the pods, per-replica certificates removed
// from the state Secrets, and Secrets deleted with their name or when
// sharing is turned off.
func TestEnvtestCertificateSharing(t *testing.T) {
	e := startEnv(t, false)
	cfg := *e.cfg
	cfg.ControllerName = "example.com/certs"
	cfg.TailnetDomain = "tail1234.ts.net"
	cfg.Defaults.CertificateSharing = true
	r := NewGatewayReconciler(e.c, e.scheme, &cfg, nil, AllFeatures(), nil)
	const domain = "gw.tail1234.ts.net"
	e.create(namespace("c"), backendService("c", "app", 80),
		&gwv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: "certs"}, Spec: gwv1.GatewayClassSpec{ControllerName: "example.com/certs"}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "c", Name: "gw"}, StringData: map[string]string{"authkey": "tskey-auth-user"}},
		&gwv1.Gateway{ObjectMeta: metav1.ObjectMeta{Namespace: "c", Name: "gw"}, Spec: gwv1.GatewaySpec{GatewayClassName: "certs",
			Listeners: []gwv1.Listener{{Name: "https", Protocol: gwv1.HTTPSProtocolType, Port: 443}}}},
		&gwv1.HTTPRoute{ObjectMeta: metav1.ObjectMeta{Namespace: "c", Name: "r"}, Spec: gwv1.HTTPRouteSpec{
			CommonRouteSpec: gwv1.CommonRouteSpec{ParentRefs: []gwv1.ParentReference{{Name: "gw"}}},
			Rules: []gwv1.HTTPRouteRule{{BackendRefs: []gwv1.HTTPBackendRef{{BackendRef: gwv1.BackendRef{
				BackendObjectReference: gwv1.BackendObjectReference{Name: "app", Port: ptr.To(gwv1.PortNumber(80))}}}}}},
		}},
	)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "c", Name: "gw"}}
	reconcile := func() {
		t.Helper()
		if _, err := r.Reconcile(e.ctx, req); err != nil {
			t.Fatal(err)
		}
	}
	podHash := func() string {
		t.Helper()
		sts := &appsv1.StatefulSet{}
		if err := e.get("c", "gw", sts); err != nil {
			t.Fatal(err)
		}
		return sts.Spec.Template.Annotations[proxy.AnnotationCertificates]
	}
	writeCert := func(cert string) {
		t.Helper()
		s := &corev1.Secret{}
		if err := e.get("c", domain, s); err != nil {
			t.Fatal(err)
		}
		s.Data = map[string][]byte{"tls.crt": []byte(cert), "tls.key": []byte("key")}
		if err := e.c.Update(e.ctx, s); err != nil {
			t.Fatal(err)
		}
	}

	reconcile()
	sec := &corev1.Secret{}
	if err := e.get("c", domain, sec); err != nil {
		t.Fatalf("certificate Secret: %v", err)
	}
	if len(sec.Data) != 0 || sec.Labels[proxy.LabelTailscaleSecretType] != "certs" || !metav1.IsControlledBy(sec, gatewayOf(e, "c", "gw")) {
		t.Errorf("certificate Secret = %+v", sec)
	}
	role := &rbacv1.Role{}
	if err := e.get("c", "gw", role); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(role.Rules[0].ResourceNames, ","); got != "gw-0,gw-1,"+domain {
		t.Errorf("Role resourceNames = %s", got)
	}
	if h := podHash(); h != "" {
		t.Errorf("pod hash before any certificate = %q", h)
	}

	// The first certificate does not restart anything; a renewal does.
	writeCert("first")
	reconcile()
	if h := podHash(); h != "" {
		t.Errorf("pod hash after the first certificate = %q", h)
	}
	writeCert("renewed")
	reconcile()
	renewed := podHash()
	if renewed == "" {
		t.Fatal("a renewal did not change the pod template")
	}
	reconcile()
	if h := podHash(); h != renewed {
		t.Errorf("pod hash changed without a renewal: %q -> %q", renewed, h)
	}
	writeCert("renewed again")
	reconcile()
	if h := podHash(); h == renewed || h == "" {
		t.Errorf("second renewal: pod hash %q", h)
	}

	// Certificates a replica issued for itself are removed from its state.
	state := &corev1.Secret{}
	if err := e.get("c", "gw-1", state); err != nil {
		t.Fatal(err)
	}
	state.Data[domain+".crt"] = []byte("old")
	state.Data[domain+".key"] = []byte("old")
	if err := e.c.Update(e.ctx, state); err != nil {
		t.Fatal(err)
	}
	reconcile()
	_ = e.get("c", "gw-1", state)
	if _, ok := state.Data[domain+".crt"]; ok || string(state.Data["authkey"]) != "tskey-auth-user" {
		t.Errorf("state Secret keys = %v", keys(state.Data))
	}

	// Turning sharing off deletes the Secret and its grant.
	cfg.Defaults.CertificateSharing = false
	reconcile()
	if err := e.get("c", domain, &corev1.Secret{}); !apierrors.IsNotFound(err) {
		t.Errorf("certificate Secret kept after turning sharing off: %v", err)
	}
	_ = e.get("c", "gw", role)
	if got := strings.Join(role.Rules[0].ResourceNames, ","); got != "gw-0,gw-1" {
		t.Errorf("Role resourceNames while off = %s", got)
	}
}

// TestEnvtestCertificateSecretConflict leaves a Secret that already has a
// certificate Secret's name alone, and grants the proxies nothing on it.
func TestEnvtestCertificateSecretConflict(t *testing.T) {
	e := startEnv(t, false)
	cfg := *e.cfg
	cfg.ControllerName = "example.com/certs"
	cfg.TailnetDomain = "tail1234.ts.net"
	cfg.Defaults.CertificateSharing = true
	r := NewGatewayReconciler(e.c, e.scheme, &cfg, nil, AllFeatures(), nil)
	e.create(namespace("x"), backendService("x", "app", 80),
		&gwv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: "certs"}, Spec: gwv1.GatewayClassSpec{ControllerName: "example.com/certs"}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "x", Name: "gw"}, StringData: map[string]string{"authkey": "tskey-auth-user"}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "x", Name: "gw.tail1234.ts.net"}, StringData: map[string]string{"mine": "yes"}},
		&gwv1.Gateway{ObjectMeta: metav1.ObjectMeta{Namespace: "x", Name: "gw"}, Spec: gwv1.GatewaySpec{GatewayClassName: "certs",
			Listeners: []gwv1.Listener{{Name: "https", Protocol: gwv1.HTTPSProtocolType, Port: 443}}}},
		&gwv1.HTTPRoute{ObjectMeta: metav1.ObjectMeta{Namespace: "x", Name: "r"}, Spec: gwv1.HTTPRouteSpec{
			CommonRouteSpec: gwv1.CommonRouteSpec{ParentRefs: []gwv1.ParentReference{{Name: "gw"}}},
			Rules: []gwv1.HTTPRouteRule{{BackendRefs: []gwv1.HTTPBackendRef{{BackendRef: gwv1.BackendRef{
				BackendObjectReference: gwv1.BackendObjectReference{Name: "app", Port: ptr.To(gwv1.PortNumber(80))}}}}}},
		}},
	)
	if _, err := r.Reconcile(e.ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "x", Name: "gw"}}); err != nil {
		t.Fatal(err)
	}
	g := gatewayOf(e, "x", "gw")
	if err := condIs(g.Status.Conditions, "Programmed", metav1.ConditionFalse); err != nil {
		t.Error(err)
	}
	role := &rbacv1.Role{}
	if err := e.get("x", "gw", role); err == nil && strings.Contains(strings.Join(role.Rules[0].ResourceNames, ","), "ts.net") {
		t.Errorf("Role grants the foreign Secret: %v", role.Rules[0].ResourceNames)
	}
	s := &corev1.Secret{}
	_ = e.get("x", "gw.tail1234.ts.net", s)
	if string(s.Data["mine"]) != "yes" || len(s.OwnerReferences) != 0 {
		t.Errorf("foreign Secret changed: %+v", s)
	}
}

func gatewayOf(e *env, ns, name string) *gwv1.Gateway {
	e.t.Helper()
	g := &gwv1.Gateway{}
	if err := e.get(ns, name, g); err != nil {
		e.t.Fatal(err)
	}
	return g
}

func keys(m map[string][]byte) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	return out
}
