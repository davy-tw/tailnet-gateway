package controller

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"

	"go.pkgs.davy.tw/tailnet-gateway/internal/config"
	"go.pkgs.davy.tw/tailnet-gateway/internal/proxy"
)

// TestEnvtestCertificateSharing covers the certificate Secrets: created
// empty per HTTPS name and granted to the proxies, their bookkeeping as
// certificates are written, a renewal restarting only the reading
// replicas that started before it once the previous certificate is about
// to expire, per-replica certificates removed from the state Secrets, a
// Secret kept while a replica may hold its certificate, and the Secrets
// deleted when sharing is turned off.
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
		rootRoute("c", "r", "gw", "app"),
	)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "c", Name: "gw"}}
	reconcile := func() ctrl.Result {
		t.Helper()
		res, err := r.Reconcile(e.ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		return res
	}
	certSecret := func() *corev1.Secret {
		t.Helper()
		s := &corev1.Secret{}
		if err := e.get("c", domain, s); err != nil {
			t.Fatal(err)
		}
		return s
	}
	writeCert := func(notAfter time.Time) {
		t.Helper()
		s := certSecret()
		s.Data = map[string][]byte{"tls.crt": testCert(t, domain, notAfter), "tls.key": []byte("key")}
		if err := e.c.Update(e.ctx, s); err != nil {
			t.Fatal(err)
		}
	}

	reconcile()
	sec := certSecret()
	if len(sec.Data) != 0 || sec.Labels[proxy.LabelTailscaleSecretType] != "certs" || !metav1.IsControlledBy(sec, gatewayOf(e, "c", "gw")) {
		t.Errorf("certificate Secret = %+v", sec)
	}
	role := &rbacv1.Role{}
	if err := e.get("c", "gw", role); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(role.Rules[0].ResourceNames, ","); got != "gw-0,gw-1,"+domain || len(role.Rules) != 2 {
		t.Errorf("Role = %+v", role.Rules)
	}

	pods := createProxyPods(e, "c", "gw", 2)
	first := time.Now().Add(90 * 24 * time.Hour).Truncate(time.Second)
	writeCert(first)
	reconcile()
	sec = certSecret()
	if sec.Annotations[proxy.AnnotationCertificateNotAfter] != first.UTC().Format(time.RFC3339) ||
		sec.Annotations[proxy.AnnotationPreviousCertificateNotAfter] != "" {
		t.Errorf("annotations after the first certificate = %v", sec.Annotations)
	}

	// A renewal whose previous certificate is far from expiring restarts
	// nothing yet, and asks to be looked at again before it does.
	second := time.Now().Add(120 * 24 * time.Hour).Truncate(time.Second)
	writeCert(second)
	reconcile()
	sec = certSecret()
	if sec.Annotations[proxy.AnnotationPreviousCertificateNotAfter] != first.UTC().Format(time.RFC3339) {
		t.Errorf("annotations after a renewal = %v", sec.Annotations)
	}
	states, _, err := r.syncCertSecrets(e.ctx, gatewayOf(e, "c", "gw"), certSpec(r, e, "c", "gw", cfg.Defaults, domain), time.Now())
	if err != nil || len(states) != 1 {
		t.Fatalf("states = %+v, %v", states, err)
	}
	again, err := r.restartStaleReaders(e.ctx, gatewayOf(e, "c", "gw"), cfg.Defaults, states, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if want := time.Until(first.Add(-certRestartMargin)); again < want-time.Minute || again > want+time.Minute {
		t.Errorf("check again in %v, want about %v", again, want)
	}
	assertDeleting(e, "c", map[string]bool{"gw-0": false, "gw-1": false})

	// The list reload restarts nothing: tailscaled reloads by itself.
	listParams := cfg.Defaults
	listParams.CertificateReload = config.CertificateReloadList
	if again, err := r.restartStaleReaders(e.ctx, gatewayOf(e, "c", "gw"), listParams, states, first); err != nil || again != 0 {
		t.Errorf("list reload: again %v, %v", again, err)
	}
	assertDeleting(e, "c", map[string]bool{"gw-0": false, "gw-1": false})

	// Once it is about to expire, the reading replica is restarted, and
	// never replica 0.
	if _, err := r.restartStaleReaders(e.ctx, gatewayOf(e, "c", "gw"), cfg.Defaults, states, first.Add(-certRestartMargin+time.Hour)); err != nil {
		t.Fatal(err)
	}
	assertDeleting(e, "c", map[string]bool{"gw-0": false, "gw-1": true})
	_ = pods
	createProxyPods(e, "c", "gw", 2) // the StatefulSet replaces it

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

	// A name no longer served stays granted for a while, as replica 0 may
	// not have its new serve config yet.
	gw := gatewayOf(e, "c", "gw")
	gw.Spec.Listeners[0].Port = 80
	gw.Spec.Listeners[0].Protocol = gwv1.HTTPProtocolType
	if err := e.c.Update(e.ctx, gw); err != nil {
		t.Fatal(err)
	}
	if res := reconcile(); res.RequeueAfter == 0 || res.RequeueAfter > certGrantGrace {
		t.Errorf("requeue after %v during the grant grace", res.RequeueAfter)
	}
	sec = certSecret()
	if sec.Annotations[annotationUnusedSince] == "" {
		t.Errorf("unused Secret annotations = %v", sec.Annotations)
	}
	_ = e.get("c", "gw", role)
	if got := strings.Join(role.Rules[0].ResourceNames, ","); got != "gw-0,gw-1,"+domain {
		t.Errorf("Role resourceNames during the grant grace = %s", got)
	}
	// Then it keeps its Secret, without a grant, while a reading replica
	// that started before may hold its certificate.
	timeNow = func() time.Time { return time.Now().Add(certGrantGrace + time.Minute) }
	t.Cleanup(func() { timeNow = time.Now })
	reconcile()
	certSecret()
	_ = e.get("c", "gw", role)
	if got := strings.Join(role.Rules[0].ResourceNames, ","); got != "gw-0,gw-1" {
		t.Errorf("Role resourceNames for an unused name = %s", got)
	}
	// It goes once that replica has been replaced.
	if err := e.c.Delete(e.ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "c", Name: "gw-1"}}, client.GracePeriodSeconds(0)); client.IgnoreNotFound(err) != nil {
		t.Fatal(err)
	}
	e.eventually("pod gw-1 gone", func() error {
		if err := e.get("c", "gw-1", &corev1.Pod{}); !apierrors.IsNotFound(err) {
			return fmt.Errorf("still there: %v", err)
		}
		return nil
	})
	createProxyPods(e, "c", "gw", 2)
	reconcile()
	if err := e.get("c", domain, &corev1.Secret{}); !apierrors.IsNotFound(err) {
		t.Errorf("unused certificate Secret kept after the replicas were replaced: %v", err)
	}

	// Turning sharing off deletes the Secrets.
	gw = gatewayOf(e, "c", "gw")
	gw.Spec.Listeners[0].Port = 443
	gw.Spec.Listeners[0].Protocol = gwv1.HTTPSProtocolType
	if err := e.c.Update(e.ctx, gw); err != nil {
		t.Fatal(err)
	}
	reconcile()
	certSecret()
	// Turned off, the Secrets stay granted while a pod (replica 0, rolled
	// last) still runs in cert share mode.
	replacePod(e, "c", "gw", 0, true)
	cfg.Defaults.CertificateSharing = false
	reconcile()
	certSecret()
	_ = e.get("c", "gw", role)
	if got := strings.Join(role.Rules[0].ResourceNames, ","); got != "gw-0,gw-1,"+domain {
		t.Errorf("Role resourceNames while replica 0 still shares = %s", got)
	}
	replacePod(e, "c", "gw", 0, false)
	reconcile()
	if err := e.get("c", domain, &corev1.Secret{}); !apierrors.IsNotFound(err) {
		t.Errorf("certificate Secret kept after turning sharing off: %v", err)
	}
	_ = e.get("c", "gw", role)
	if got := strings.Join(role.Rules[0].ResourceNames, ","); got != "gw-0,gw-1" {
		t.Errorf("Role resourceNames while off = %s", got)
	}
}

// TestEnvtestCertificateSeeding fills a new shared Secret with a valid
// certificate a replica issued for itself, replica 0's first, before the
// state Secrets are stripped.
func TestEnvtestCertificateSeeding(t *testing.T) {
	e := startEnv(t, false)
	cfg := *e.cfg
	cfg.ControllerName = "example.com/certs"
	cfg.TailnetDomain = "tail1234.ts.net"
	r := NewGatewayReconciler(e.c, e.scheme, &cfg, nil, AllFeatures(), nil)
	const domain = "gw.tail1234.ts.net"
	e.create(namespace("s"), backendService("s", "app", 80),
		&gwv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: "certs"}, Spec: gwv1.GatewayClassSpec{ControllerName: "example.com/certs"}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "s", Name: "gw"}, StringData: map[string]string{"authkey": "tskey-auth-user"}},
		&gwv1.Gateway{ObjectMeta: metav1.ObjectMeta{Namespace: "s", Name: "gw"}, Spec: gwv1.GatewaySpec{GatewayClassName: "certs",
			Listeners: []gwv1.Listener{{Name: "https", Protocol: gwv1.HTTPSProtocolType, Port: 443}}}},
		rootRoute("s", "r", "gw", "app"),
	)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "s", Name: "gw"}}
	if _, err := r.Reconcile(e.ctx, req); err != nil {
		t.Fatal(err)
	}
	certs := map[int][]byte{
		0: testCert(t, domain, time.Now().Add(80*24*time.Hour)),
		1: testCert(t, domain, time.Now().Add(85*24*time.Hour)),
	}
	for i, c := range certs {
		s := &corev1.Secret{}
		if err := e.get("s", proxy.StateSecretName("gw", i), s); err != nil {
			t.Fatal(err)
		}
		s.Data[domain+".crt"] = c
		s.Data[domain+".key"] = []byte(fmt.Sprintf("key%d", i))
		if err := e.c.Update(e.ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	cfg.Defaults.CertificateSharing = true
	if _, err := r.Reconcile(e.ctx, req); err != nil {
		t.Fatal(err)
	}
	sec := &corev1.Secret{}
	if err := e.get("s", domain, sec); err != nil {
		t.Fatal(err)
	}
	if string(sec.Data["tls.crt"]) != string(certs[0]) || string(sec.Data["tls.key"]) != "key0" {
		t.Errorf("shared Secret not seeded from replica 0: %v", keys(sec.Data))
	}
	for i := range certs {
		s := &corev1.Secret{}
		_ = e.get("s", proxy.StateSecretName("gw", i), s)
		if _, ok := s.Data[domain+".crt"]; ok {
			t.Errorf("replica %d keeps its certificate", i)
		}
	}
	// The controller's later applies leave the seeded certificate alone.
	if _, err := r.Reconcile(e.ctx, req); err != nil {
		t.Fatal(err)
	}
	_ = e.get("s", domain, sec)
	if string(sec.Data["tls.crt"]) != string(certs[0]) || sec.Annotations[proxy.AnnotationCertificateSHA256] == "" {
		t.Errorf("seeded Secret after another reconcile: data %v, annotations %v", keys(sec.Data), sec.Annotations)
	}
}

// TestEnvtestCertificateListReload grants list with the list reload.
func TestEnvtestCertificateListReload(t *testing.T) {
	e := startEnv(t, false)
	cfg := *e.cfg
	cfg.ControllerName = "example.com/certs"
	cfg.TailnetDomain = "tail1234.ts.net"
	cfg.Defaults.CertificateSharing = true
	cfg.Defaults.CertificateReload = config.CertificateReloadList
	r := NewGatewayReconciler(e.c, e.scheme, &cfg, nil, AllFeatures(), nil)
	e.create(namespace("l"), backendService("l", "app", 80),
		&gwv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: "certs"}, Spec: gwv1.GatewayClassSpec{ControllerName: "example.com/certs"}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "l", Name: "gw"}, StringData: map[string]string{"authkey": "tskey-auth-user"}},
		&gwv1.Gateway{ObjectMeta: metav1.ObjectMeta{Namespace: "l", Name: "gw"}, Spec: gwv1.GatewaySpec{GatewayClassName: "certs",
			Listeners: []gwv1.Listener{{Name: "https", Protocol: gwv1.HTTPSProtocolType, Port: 443}}}},
		rootRoute("l", "r", "gw", "app"),
	)
	if _, err := r.Reconcile(e.ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "l", Name: "gw"}}); err != nil {
		t.Fatal(err)
	}
	role := &rbacv1.Role{}
	if err := e.get("l", "gw", role); err != nil {
		t.Fatal(err)
	}
	if len(role.Rules) != 3 || role.Rules[2].Verbs[0] != "list" || len(role.Rules[2].ResourceNames) != 0 {
		t.Errorf("Role = %+v", role.Rules)
	}
}

func rootRoute(ns, name, gateway, backend string) *gwv1.HTTPRoute {
	return &gwv1.HTTPRoute{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name}, Spec: gwv1.HTTPRouteSpec{
		CommonRouteSpec: gwv1.CommonRouteSpec{ParentRefs: []gwv1.ParentReference{{Name: gwv1.ObjectName(gateway)}}},
		Rules: []gwv1.HTTPRouteRule{{BackendRefs: []gwv1.HTTPBackendRef{{BackendRef: gwv1.BackendRef{
			BackendObjectReference: gwv1.BackendObjectReference{Name: gwv1.ObjectName(backend), Port: ptr.To(gwv1.PortNumber(80))}}}}}},
	}}
}

// replacePod replaces a proxy pod, in cert share mode or not.
func replacePod(e *env, ns, gw string, ord int, shares bool) {
	e.t.Helper()
	name := fmt.Sprintf("%s-%d", gw, ord)
	if err := e.c.Delete(e.ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name}}, client.GracePeriodSeconds(0)); client.IgnoreNotFound(err) != nil {
		e.t.Fatal(err)
	}
	e.eventually("pod "+name+" gone", func() error {
		if err := e.get(ns, name, &corev1.Pod{}); !apierrors.IsNotFound(err) {
			return fmt.Errorf("still there: %v", err)
		}
		return nil
	})
	createProxyPods(e, ns, gw, ord+1, shares)
}

// createProxyPods stands in for the StatefulSet controller: it creates the
// missing replicas as ready pods, in cert share mode if shares is true.
func createProxyPods(e *env, ns, gw string, n int, shares ...bool) []*corev1.Pod {
	e.t.Helper()
	var out []*corev1.Pod
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("%s-%d", gw, i)
		p := &corev1.Pod{}
		if err := e.get(ns, name, p); err == nil {
			out = append(out, p)
			continue
		}
		p = &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, Labels: proxy.SelectorLabels(gw),
				OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "StatefulSet", Name: gw, UID: "sts", Controller: ptr.To(true)}}},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "tailscale", Image: "img"}}},
		}
		if len(shares) > 0 && shares[0] {
			p.Spec.Containers[0].Env = []corev1.EnvVar{{Name: proxy.EnvCertShare, Value: "true"}}
		}
		e.create(p)
		p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
		if err := e.c.Status().Update(e.ctx, p); err != nil {
			e.t.Fatal(err)
		}
		out = append(out, p)
	}
	// Creation timestamps have a one-second resolution.
	time.Sleep(1100 * time.Millisecond)
	return out
}

func assertDeleting(e *env, ns string, want map[string]bool) {
	e.t.Helper()
	for name, deleting := range want {
		p := &corev1.Pod{}
		err := e.get(ns, name, p)
		got := apierrors.IsNotFound(err) || (err == nil && !p.DeletionTimestamp.IsZero())
		if got != deleting {
			e.t.Errorf("pod %s being deleted = %v, want %v (%v)", name, got, deleting, err)
		}
	}
}

// certSpec is the proxy.Spec that provision builds, as far as
// syncCertSecrets needs it.
func certSpec(r *GatewayReconciler, e *env, ns, gw string, params config.ClassParams, domains ...string) *proxy.Spec {
	g := gatewayOf(e, ns, gw)
	return &proxy.Spec{Namespace: ns, Gateway: gw, Params: params, Owner: ownerRef(g), CertDomains: domains}
}

// testCert returns a self-signed PEM certificate for name.
func testCert(t *testing.T, name string, notAfter time.Time) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: name},
		DNSNames: []string{name}, NotBefore: time.Now().Add(-time.Hour), NotAfter: notAfter}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
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
