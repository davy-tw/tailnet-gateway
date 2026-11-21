package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlconfig "sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"

	"go.pkgs.davy.tw/tailnet-gateway/internal/config"
	"go.pkgs.davy.tw/tailnet-gateway/internal/proxy"
	"go.pkgs.davy.tw/tailnet-gateway/internal/serveconf"
	"go.pkgs.davy.tw/tailnet-gateway/internal/tailscale"
	"go.pkgs.davy.tw/tailnet-gateway/internal/translate"
)

// fakeTS is an in-memory Tailscale API.
type fakeTS struct {
	mu      sync.Mutex
	keys    []tailscale.KeyRequest
	devices map[string]bool
	deleted []string
	failKey error
}

func (f *fakeTS) CreateAuthKey(_ context.Context, req tailscale.KeyRequest) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failKey != nil {
		return "", f.failKey
	}
	f.keys = append(f.keys, req)
	return fmt.Sprintf("tskey-auth-%d", len(f.keys)), nil
}

func (f *fakeTS) DeleteDevice(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.devices, id)
	f.deleted = append(f.deleted, id)
	return nil
}

func (f *fakeTS) DeviceExists(_ context.Context, id string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.devices[id], nil
}

func (f *fakeTS) deletedDevices() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.deleted...)
}

type env struct {
	t      *testing.T
	ctx    context.Context
	c      client.Client
	scheme *runtime.Scheme
	ts     *fakeTS
	cfg    *config.Config
}

func gatewayAPICRDs(t *testing.T) string {
	out, err := exec.Command("go", "list", "-m", "-f", "{{.Dir}}", "sigs.k8s.io/gateway-api").Output()
	if err != nil {
		t.Fatalf("locating gateway-api module: %v", err)
	}
	src := filepath.Join(strings.TrimSpace(string(out)), "config", "crd", "standard")
	dst := t.TempDir()
	entries, err := os.ReadDir(src)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.Contains(e.Name(), "vap") {
			continue // not a CRD
		}
		b, err := os.ReadFile(filepath.Join(src, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dst, e.Name()), b, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return dst
}

func startEnv(t *testing.T, withManager bool) *env {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; run `make test-envtest`")
	}
	ctrl.SetLogger(zap.New(zap.UseDevMode(true), zap.WriteTo(os.Stderr)))
	te := &envtest.Environment{CRDDirectoryPaths: []string{gatewayAPICRDs(t)}, ErrorIfCRDPathMissing: true}
	restCfg, err := te.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = te.Stop() })

	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(gwv1.Install(scheme))

	cfg, err := config.Load(func(k string) string {
		return map[string]string{config.EnvTags: "tag:k8s"}[k]
	})
	if err != nil {
		t.Fatal(err)
	}
	ts := &fakeTS{devices: map[string]bool{}}
	mgr, err := ctrl.NewManager(restCfg, ctrl.Options{Scheme: scheme, Metrics: metricsserver.Options{BindAddress: "0"},
		Controller: ctrlconfig.Controller{SkipNameValidation: ptr.To(true)},
		Client: client.Options{Cache: &client.CacheOptions{DisableFor: []client.Object{
			&corev1.Pod{}, &corev1.ServiceAccount{}, &rbacv1.Role{}, &rbacv1.RoleBinding{}, &policyv1.PodDisruptionBudget{},
		}}}})
	if err != nil {
		t.Fatal(err)
	}
	c := mgr.GetClient()
	rec := mgr.GetEventRecorder("test")
	must := func(err error) {
		if err != nil {
			t.Fatal(err)
		}
	}
	if withManager {
		must(NewGatewayClassReconciler(c, cfg, rec).SetupWithManager(mgr))
		must(NewGatewayReconciler(c, scheme, cfg, ts, AllFeatures(), rec).SetupWithManager(mgr))
		must(NewIngressReconciler(c, scheme, cfg, rec).SetupWithManager(mgr))
		must(NewServiceReconciler(c, scheme, cfg, AllFeatures(), rec).SetupWithManager(mgr))
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = mgr.Start(ctx) }()
	if !mgr.GetCache().WaitForCacheSync(ctx) {
		t.Fatal("cache did not sync")
	}
	direct, err := client.New(restCfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	return &env{t: t, ctx: ctx, c: direct, scheme: scheme, ts: ts, cfg: cfg}
}

func (e *env) create(objs ...client.Object) {
	e.t.Helper()
	for _, o := range objs {
		if err := e.c.Create(e.ctx, o); err != nil {
			e.t.Fatalf("creating %T %s: %v", o, o.GetName(), err)
		}
	}
}

func (e *env) eventually(what string, f func() error) {
	e.t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var err error
	for time.Now().Before(deadline) {
		if err = f(); err == nil {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	e.t.Fatalf("%s: %v", what, err)
}

func (e *env) get(ns, name string, obj client.Object) error {
	return e.c.Get(e.ctx, types.NamespacedName{Namespace: ns, Name: name}, obj)
}

func condIs(conds []metav1.Condition, typ string, status metav1.ConditionStatus) error {
	c := meta.FindStatusCondition(conds, typ)
	if c == nil {
		return fmt.Errorf("no %s condition in %+v", typ, conds)
	}
	if c.Status != status {
		return fmt.Errorf("%s = %s (%s: %s)", typ, c.Status, c.Reason, c.Message)
	}
	return nil
}

func namespace(name string) *corev1.Namespace {
	return &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
}

func backendService(ns, name string, ports ...int32) *corev1.Service {
	s := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name}}
	for _, p := range ports {
		s.Spec.Ports = append(s.Spec.Ports, corev1.ServicePort{Name: fmt.Sprintf("p%d", p), Port: p})
	}
	return s
}

func TestEnvtestGatewayLifecycle(t *testing.T) {
	e := startEnv(t, true)
	e.create(namespace("demo"),
		&gwv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: "tailnet"}, Spec: gwv1.GatewayClassSpec{ControllerName: gwv1.GatewayController(config.DefaultControllerName)}},
		backendService("demo", "api", 8080), backendService("demo", "web", 80))

	e.eventually("class accepted", func() error {
		gc := &gwv1.GatewayClass{}
		if err := e.get("", "tailnet", gc); err != nil {
			return err
		}
		return condIs(gc.Status.Conditions, "Accepted", metav1.ConditionTrue)
	})

	gw := &gwv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Namespace: "demo", Name: "demo", Labels: map[string]string{"app": "web"}},
		Spec: gwv1.GatewaySpec{GatewayClassName: "tailnet", Infrastructure: &gwv1.GatewayInfrastructure{
			Labels: map[gwv1.LabelKey]gwv1.LabelValue{"team": "a"},
		}, Listeners: []gwv1.Listener{
			{Name: "http", Protocol: gwv1.HTTPProtocolType, Port: 80},
		}},
	}
	route := &gwv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{Namespace: "demo", Name: "demo"},
		Spec: gwv1.HTTPRouteSpec{
			CommonRouteSpec: gwv1.CommonRouteSpec{ParentRefs: []gwv1.ParentReference{{Name: "demo"}}},
			Hostnames:       []gwv1.Hostname{"demo"},
			Rules: []gwv1.HTTPRouteRule{
				{Matches: []gwv1.HTTPRouteMatch{{Path: &gwv1.HTTPPathMatch{Type: ptr.To(gwv1.PathMatchPathPrefix), Value: ptr.To("/api")}}},
					BackendRefs: []gwv1.HTTPBackendRef{{BackendRef: gwv1.BackendRef{BackendObjectReference: gwv1.BackendObjectReference{Name: "api", Port: ptr.To(gwv1.PortNumber(8080))}}}}},
				{BackendRefs: []gwv1.HTTPBackendRef{{BackendRef: gwv1.BackendRef{BackendObjectReference: gwv1.BackendObjectReference{Name: "web", Port: ptr.To(gwv1.PortNumber(80))}}}}},
			},
		},
	}
	e.create(gw, route)

	// Data plane objects.
	e.eventually("data plane created", func() error {
		for _, o := range []client.Object{&corev1.ServiceAccount{}, &rbacv1.Role{}, &rbacv1.RoleBinding{}, &corev1.ConfigMap{},
			&appsv1.StatefulSet{}, &policyv1.PodDisruptionBudget{}, &corev1.Secret{}} {
			name := "demo"
			if _, ok := o.(*corev1.Secret); ok {
				name = "demo-1"
			}
			if err := e.get("demo", name, o); err != nil {
				return fmt.Errorf("%T: %w", o, err)
			}
		}
		return nil
	})
	sts := &appsv1.StatefulSet{}
	_ = e.get("demo", "demo", sts)
	if *sts.Spec.Replicas != 2 || sts.Spec.PodManagementPolicy != appsv1.ParallelPodManagement {
		t.Errorf("statefulset spec: replicas=%d policy=%s", *sts.Spec.Replicas, sts.Spec.PodManagementPolicy)
	}
	if sts.Spec.Template.Labels["team"] != "a" || sts.Spec.Selector.MatchLabels["team"] != "" {
		t.Errorf("infrastructure labels must reach pods but not the selector: %v / %v", sts.Spec.Template.Labels, sts.Spec.Selector.MatchLabels)
	}
	if _, ok := sts.Spec.Template.Labels["app"]; ok {
		t.Errorf("the Gateway's own labels must not reach pods: %v", sts.Spec.Template.Labels)
	}
	role := &rbacv1.Role{}
	_ = e.get("demo", "demo", role)
	if got := strings.Join(role.Rules[0].ResourceNames, ","); got != "demo-0,demo-1" {
		t.Errorf("role resourceNames = %s", got)
	}
	for i := 0; i < 2; i++ {
		s := &corev1.Secret{}
		_ = e.get("demo", proxy.StateSecretName("demo", i), s)
		if !strings.HasPrefix(string(s.Data["authkey"]), "tskey-auth-") {
			t.Errorf("replica %d authkey = %q", i, s.Data["authkey"])
		}
	}
	if len(e.ts.keys) != 2 || strings.Join(e.ts.keys[0].Tags, ",") != "tag:k8s" {
		t.Errorf("minted keys = %+v", e.ts.keys)
	}

	// Not programmed yet: no ready replica, tailnet domain unknown.
	e.eventually("gateway pending", func() error {
		g := &gwv1.Gateway{}
		if err := e.get("demo", "demo", g); err != nil {
			return err
		}
		if err := condIs(g.Status.Conditions, "Accepted", metav1.ConditionTrue); err != nil {
			return err
		}
		return condIs(g.Status.Conditions, "Programmed", metav1.ConditionFalse)
	})
	e.eventually("route accepted", func() error {
		r := &gwv1.HTTPRoute{}
		if err := e.get("demo", "demo", r); err != nil {
			return err
		}
		if len(r.Status.Parents) != 1 {
			return fmt.Errorf("parents = %+v", r.Status.Parents)
		}
		return condIs(r.Status.Parents[0].Conditions, "Accepted", metav1.ConditionTrue)
	})

	// A replica logs in: it writes its identity, and the pods become ready.
	for i := 0; i < 2; i++ {
		s := &corev1.Secret{}
		_ = e.get("demo", proxy.StateSecretName("demo", i), s)
		s.Data["device_id"] = []byte(fmt.Sprintf("node%d", i))
		s.Data["device_fqdn"] = []byte(fmt.Sprintf("demo-demo-%d.tail1234.ts.net.", i))
		if err := e.c.Update(e.ctx, s); err != nil {
			t.Fatal(err)
		}
		e.ts.devices[fmt.Sprintf("node%d", i)] = true
	}
	_ = e.get("demo", "demo", sts)
	sts.Status.Replicas, sts.Status.ReadyReplicas, sts.Status.ObservedGeneration = 2, 2, sts.Generation
	if err := e.c.Status().Update(e.ctx, sts); err != nil {
		t.Fatal(err)
	}
	e.eventually("gateway programmed", func() error {
		g := &gwv1.Gateway{}
		if err := e.get("demo", "demo", g); err != nil {
			return err
		}
		if err := condIs(g.Status.Conditions, "Programmed", metav1.ConditionTrue); err != nil {
			return err
		}
		if len(g.Status.Addresses) != 1 || g.Status.Addresses[0].Value != "demo.tail1234.ts.net" {
			return fmt.Errorf("addresses = %+v", g.Status.Addresses)
		}
		if len(g.Status.Listeners) != 1 || g.Status.Listeners[0].AttachedRoutes != 1 {
			return fmt.Errorf("listeners = %+v", g.Status.Listeners)
		}
		return condIs(g.Status.Listeners[0].Conditions, "Programmed", metav1.ConditionTrue)
	})
	cm := &corev1.ConfigMap{}
	_ = e.get("demo", "demo", cm)
	var sc serveconf.Config
	if err := json.Unmarshal([]byte(cm.Data[proxy.ServeConfigKey]), &sc); err != nil {
		t.Fatal(err)
	}
	web := sc.Services["svc:demo"].Web["demo.tail1234.ts.net:80"]
	if web == nil || web.Handlers["/api"].Proxy != "http://api.demo.svc.cluster.local:8080/api" {
		t.Errorf("serve config = %s", cm.Data[proxy.ServeConfigKey])
	}

	// Scale to one replica through class parameters.
	e.create(&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "demo", Name: "tailnet-params"}, Data: map[string]string{"replicas": "1"}})
	gc := &gwv1.GatewayClass{}
	_ = e.get("", "tailnet", gc)
	gc.Spec.ParametersRef = &gwv1.ParametersReference{Group: "", Kind: "ConfigMap", Name: "tailnet-params", Namespace: ptr.To(gwv1.Namespace("demo"))}
	if err := e.c.Update(e.ctx, gc); err != nil {
		t.Fatal(err)
	}
	e.eventually("scaled down", func() error {
		s := &appsv1.StatefulSet{}
		if err := e.get("demo", "demo", s); err != nil {
			return err
		}
		if *s.Spec.Replicas != 1 {
			return fmt.Errorf("replicas = %d", *s.Spec.Replicas)
		}
		if err := e.get("demo", "demo-1", &corev1.Secret{}); !apierrors.IsNotFound(err) {
			return fmt.Errorf("state secret demo-1 still there: %v", err)
		}
		if err := e.get("demo", "demo", &policyv1.PodDisruptionBudget{}); !apierrors.IsNotFound(err) {
			return fmt.Errorf("PDB still there: %v", err)
		}
		return nil
	})
	if got := e.ts.deletedDevices(); len(got) != 1 || got[0] != "node1" {
		t.Errorf("deleted devices = %v", got)
	}

	// Invalid parameters make the class and its Gateways not accepted.
	cmp := &corev1.ConfigMap{}
	_ = e.get("demo", "tailnet-params", cmp)
	cmp.Data["replicas"] = "lots"
	if err := e.c.Update(e.ctx, cmp); err != nil {
		t.Fatal(err)
	}
	e.eventually("class invalid", func() error {
		gc := &gwv1.GatewayClass{}
		if err := e.get("", "tailnet", gc); err != nil {
			return err
		}
		return condIs(gc.Status.Conditions, "Accepted", metav1.ConditionFalse)
	})
	e.eventually("gateway invalid parameters", func() error {
		g := &gwv1.Gateway{}
		if err := e.get("demo", "demo", g); err != nil {
			return err
		}
		c := meta.FindStatusCondition(g.Status.Conditions, "Accepted")
		if c == nil || c.Reason != "InvalidParameters" {
			return fmt.Errorf("accepted = %+v", c)
		}
		// A rejected Gateway stops serving.
		if err := e.get("demo", "demo", &appsv1.StatefulSet{}); !apierrors.IsNotFound(err) {
			return fmt.Errorf("statefulset still there: %v", err)
		}
		return nil
	})
	cmp.Data["replicas"] = "1"
	if err := e.c.Update(e.ctx, cmp); err != nil {
		t.Fatal(err)
	}

	// Deleting the Gateway deletes its devices and lets it go.
	if err := e.c.Delete(e.ctx, gw); err != nil {
		t.Fatal(err)
	}
	e.eventually("gateway gone", func() error {
		if err := e.get("demo", "demo", &gwv1.Gateway{}); !apierrors.IsNotFound(err) {
			return fmt.Errorf("still there: %v", err)
		}
		return nil
	})
	// Deletion is idempotent, so a repeated finalizer run may delete twice.
	if got := e.ts.deletedDevices(); len(got) < 2 || got[1] != "node0" {
		t.Errorf("deleted devices = %v", got)
	}
	e.eventually("route status cleared", func() error {
		r := &gwv1.HTTPRoute{}
		if err := e.get("demo", "demo", r); err != nil {
			return err
		}
		if len(r.Status.Parents) != 0 {
			return fmt.Errorf("parents = %+v", r.Status.Parents)
		}
		return nil
	})
}

func TestEnvtestIngressAndService(t *testing.T) {
	e := startEnv(t, true)
	e.create(namespace("apps"),
		&gwv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: "tailnet"}, Spec: gwv1.GatewayClassSpec{ControllerName: gwv1.GatewayController(config.DefaultControllerName)}},
		&networkingv1.IngressClass{ObjectMeta: metav1.ObjectMeta{Name: "tailnet"}, Spec: networkingv1.IngressClassSpec{Controller: config.IngressControllerName}},
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: "apps", Name: "web"},
			Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Name: "http", Port: 8080}}}},
	)
	prefix := networkingv1.PathTypePrefix
	ing := &networkingv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{Namespace: "apps", Name: "site"},
		Spec: networkingv1.IngressSpec{
			IngressClassName: ptr.To("tailnet"),
			TLS:              []networkingv1.IngressTLS{{Hosts: []string{"site"}}},
			Rules: []networkingv1.IngressRule{{Host: "site", IngressRuleValue: networkingv1.IngressRuleValue{HTTP: &networkingv1.HTTPIngressRuleValue{
				Paths: []networkingv1.HTTPIngressPath{{Path: "/", PathType: &prefix, Backend: networkingv1.IngressBackend{
					Service: &networkingv1.IngressServiceBackend{Name: "web", Port: networkingv1.ServiceBackendPort{Name: "http"}},
				}}},
			}}}},
		},
	}
	e.create(ing)
	e.eventually("ingress translated", func() error {
		g := &gwv1.Gateway{}
		if err := e.get("apps", "site", g); err != nil {
			return err
		}
		if l := g.Spec.Listeners; len(l) != 1 || l[0].Protocol != gwv1.HTTPSProtocolType || l[0].Port != 443 {
			return fmt.Errorf("listeners = %+v", l)
		}
		r := &gwv1.HTTPRoute{}
		if err := e.get("apps", "site", r); err != nil {
			return err
		}
		if p := r.Spec.Rules[0].BackendRefs[0].Port; p == nil || *p != 8080 {
			return fmt.Errorf("named port not resolved: %v", p)
		}
		return nil
	})

	// The default backend takes the paths and hosts that no rule matches.
	backend := networkingv1.IngressBackend{Service: &networkingv1.IngressServiceBackend{Name: "web", Port: networkingv1.ServiceBackendPort{Number: 8080}}}
	e.create(&networkingv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{Namespace: "apps", Name: "fallback"},
		Spec: networkingv1.IngressSpec{
			IngressClassName: ptr.To("tailnet"),
			DefaultBackend:   &backend,
			Rules: []networkingv1.IngressRule{{Host: "app", IngressRuleValue: networkingv1.IngressRuleValue{HTTP: &networkingv1.HTTPIngressRuleValue{
				Paths: []networkingv1.HTTPIngressPath{{Path: "/api", PathType: &prefix, Backend: backend}},
			}}}},
		},
	})
	e.eventually("default backend added", func() error {
		host, other := &gwv1.HTTPRoute{}, &gwv1.HTTPRoute{}
		if err := e.get("apps", "fallback-0", host); err != nil {
			return err
		}
		if err := e.get("apps", "fallback-1", other); err != nil {
			return err
		}
		if len(host.Spec.Rules) != 2 || *host.Spec.Rules[1].Matches[0].Path.Value != "/" {
			return fmt.Errorf("host rules = %+v", host.Spec.Rules)
		}
		if len(other.Spec.Hostnames) != 0 || len(other.Spec.Rules) != 1 {
			return fmt.Errorf("catch-all route = %+v", other.Spec)
		}
		return nil
	})

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: "apps", Name: "db", Annotations: map[string]string{config.AnnotationHostname: "db"}},
		Spec: corev1.ServiceSpec{
			Type:              corev1.ServiceTypeLoadBalancer,
			LoadBalancerClass: ptr.To("tailnet"),
			Ports: []corev1.ServicePort{
				{Name: "ui", Port: 80, AppProtocol: ptr.To("http")},
				{Name: "pg", Port: 5432},
				{Name: "dns", Port: 53, Protocol: corev1.ProtocolUDP},
			},
		},
	}
	e.create(svc)
	e.eventually("service translated", func() error {
		g := &gwv1.Gateway{}
		if err := e.get("apps", "db", g); err != nil {
			return err
		}
		var names []string
		for _, l := range g.Spec.Listeners {
			names = append(names, string(l.Name))
		}
		if strings.Join(names, ",") != "http-80,tcp-5432" {
			return fmt.Errorf("listeners = %v", names)
		}
		if err := e.get("apps", "db-http-80", &gwv1.HTTPRoute{}); err != nil {
			return err
		}
		tr := &gwv1.TCPRoute{}
		if err := e.get("apps", "db-tcp-5432", tr); err != nil {
			return err
		}
		if tr.Annotations[config.AnnotationTailscaleService] != "db" {
			return fmt.Errorf("annotations = %v", tr.Annotations)
		}
		s := &corev1.Service{}
		if err := e.get("apps", "db", s); err != nil {
			return err
		}
		if len(s.Finalizers) != 1 {
			return fmt.Errorf("finalizers = %v", s.Finalizers)
		}
		return nil
	})

	// Removing a port prunes its route.
	s := &corev1.Service{}
	_ = e.get("apps", "db", s)
	s.Spec.Ports = s.Spec.Ports[:1]
	if err := e.c.Update(e.ctx, s); err != nil {
		t.Fatal(err)
	}
	e.eventually("tcp route pruned", func() error {
		if err := e.get("apps", "db-tcp-5432", &gwv1.TCPRoute{}); !apierrors.IsNotFound(err) {
			return fmt.Errorf("still there: %v", err)
		}
		return nil
	})

	// Changing the class away from ours cleans up and drops the finalizer.
	_ = e.get("apps", "db", s)
	s.Spec.Type = corev1.ServiceTypeClusterIP
	s.Spec.LoadBalancerClass = nil
	if err := e.c.Update(e.ctx, s); err != nil {
		t.Fatal(err)
	}
	e.eventually("service released", func() error {
		s := &corev1.Service{}
		if err := e.get("apps", "db", s); err != nil {
			return err
		}
		if len(s.Finalizers) != 0 {
			return fmt.Errorf("finalizers = %v", s.Finalizers)
		}
		if err := e.get("apps", "db", &gwv1.Gateway{}); !apierrors.IsNotFound(err) {
			return fmt.Errorf("gateway still there: %v", err)
		}
		return nil
	})
}

// TestEnvtestManualKeyMode drives the reconciler by hand, without a
// manager, so that each step is deterministic.
func TestEnvtestManualKeyMode(t *testing.T) {
	e := startEnv(t, false)
	cfg := *e.cfg
	cfg.ControllerName = "example.com/manual"
	r := NewGatewayReconciler(e.c, e.scheme, &cfg, nil, AllFeatures(), nil)
	e.create(namespace("m"),
		&gwv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: "manual"}, Spec: gwv1.GatewayClassSpec{ControllerName: "example.com/manual"}},
		&gwv1.Gateway{ObjectMeta: metav1.ObjectMeta{Namespace: "m", Name: "gw"}, Spec: gwv1.GatewaySpec{GatewayClassName: "manual",
			Listeners: []gwv1.Listener{{Name: "tcp", Protocol: gwv1.TCPProtocolType, Port: 22}}}},
	)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "m", Name: "gw"}}
	if _, err := r.Reconcile(e.ctx, req); err != nil {
		t.Fatal(err)
	}
	g := &gwv1.Gateway{}
	_ = e.get("m", "gw", g)
	c := meta.FindStatusCondition(g.Status.Conditions, "Programmed")
	if c == nil || c.Status != metav1.ConditionFalse || !strings.Contains(c.Message, "authkey") {
		t.Fatalf("programmed = %+v", c)
	}
	if err := e.get("m", "gw", &appsv1.StatefulSet{}); !apierrors.IsNotFound(err) {
		t.Errorf("no StatefulSet expected without a key: %v", err)
	}

	e.create(&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "m", Name: "gw"}, StringData: map[string]string{"authkey": "tskey-auth-user"}})
	if _, err := r.Reconcile(e.ctx, req); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		s := &corev1.Secret{}
		if err := e.get("m", proxy.StateSecretName("gw", i), s); err != nil {
			t.Fatal(err)
		}
		if string(s.Data["authkey"]) != "tskey-auth-user" {
			t.Errorf("replica %d authkey = %q", i, s.Data["authkey"])
		}
	}
	if err := e.get("m", "gw", &appsv1.StatefulSet{}); err != nil {
		t.Errorf("StatefulSet: %v", err)
	}
	// A key already in a state Secret is never overwritten.
	user := &corev1.Secret{}
	_ = e.get("m", "gw", user)
	user.Data["authkey"] = []byte("tskey-auth-rotated")
	if err := e.c.Update(e.ctx, user); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(e.ctx, req); err != nil {
		t.Fatal(err)
	}
	s := &corev1.Secret{}
	_ = e.get("m", "gw-0", s)
	if string(s.Data["authkey"]) != "tskey-auth-user" {
		t.Errorf("authkey was overwritten: %q", s.Data["authkey"])
	}
}

func httpGateway(ns, name string) *gwv1.Gateway {
	return &gwv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec: gwv1.GatewaySpec{GatewayClassName: "tailnet", Listeners: []gwv1.Listener{
			{Name: "http", Protocol: gwv1.HTTPProtocolType, Port: 80, AllowedRoutes: &gwv1.AllowedRoutes{
				Namespaces: &gwv1.RouteNamespaces{From: ptr.To(gwv1.NamespacesFromSame)}}},
		}},
	}
}

func hostRoute(ns, name, gateway, host, backend string) *gwv1.HTTPRoute {
	return &gwv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec: gwv1.HTTPRouteSpec{
			CommonRouteSpec: gwv1.CommonRouteSpec{ParentRefs: []gwv1.ParentReference{{Name: gwv1.ObjectName(gateway)}}},
			Hostnames:       []gwv1.Hostname{gwv1.Hostname(host)},
			Rules: []gwv1.HTTPRouteRule{{BackendRefs: []gwv1.HTTPBackendRef{{BackendRef: gwv1.BackendRef{
				BackendObjectReference: gwv1.BackendObjectReference{Name: gwv1.ObjectName(backend), Port: ptr.To(gwv1.PortNumber(80))}}}}}},
		},
	}
}

func (e *env) serveConfig(ns, name string) (serveconf.Config, error) {
	var sc serveconf.Config
	cm := &corev1.ConfigMap{}
	if err := e.get(ns, name, cm); err != nil {
		return sc, err
	}
	err := json.Unmarshal([]byte(cm.Data[proxy.ServeConfigKey]), &sc)
	return sc, err
}

// TestEnvtestOwnership covers objects and names that belong to someone
// else: existing objects, Tailscale Service names of other Gateways, and
// IngressClasses of other controllers.
func TestEnvtestOwnership(t *testing.T) {
	e := startEnv(t, true)
	e.cfg.TailnetDomain = "tail1234.ts.net" // set before any object exists
	e.create(namespace("one"), namespace("two"),
		&gwv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: "tailnet"}, Spec: gwv1.GatewayClassSpec{ControllerName: gwv1.GatewayController(config.DefaultControllerName)}},
		backendService("one", "web", 80), backendService("two", "web", 80),
		// Not created by the controller: must not be taken over.
		&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: "one", Name: "taken"}},
	)

	e.create(httpGateway("one", "taken"))
	e.eventually("existing object reported", func() error {
		g := &gwv1.Gateway{}
		if err := e.get("one", "taken", g); err != nil {
			return err
		}
		c := meta.FindStatusCondition(g.Status.Conditions, "Programmed")
		if c == nil || c.Status != metav1.ConditionFalse || !strings.Contains(c.Message, "ServiceAccount one/taken exists and is not owned") {
			return fmt.Errorf("programmed = %+v", c)
		}
		return nil
	})
	sa := &corev1.ServiceAccount{}
	_ = e.get("one", "taken", sa)
	if len(sa.OwnerReferences) != 0 {
		t.Errorf("ServiceAccount was taken over: %+v", sa.OwnerReferences)
	}

	// The UID sent with an apply is a precondition: a replaced object is
	// not taken over.
	e.create(&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "one", Name: "uid-check"}})
	err := applyWithGVK(e.ctx, e.c, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "one", Name: "uid-check"}, Data: map[string]string{"a": "b"}},
		corev1.SchemeGroupVersion.WithKind("ConfigMap"), "00000000-0000-0000-0000-000000000000")
	if err == nil {
		t.Error("apply with another object's UID must fail")
	}

	// The older Gateway keeps a Tailscale Service name.
	e.create(httpGateway("one", "first"), hostRoute("one", "r", "first", "shared", "web"))
	e.eventually("first serves svc:shared", func() error {
		sc, err := e.serveConfig("one", "first")
		if err != nil {
			return err
		}
		if _, ok := sc.Services["svc:shared"]; !ok {
			return fmt.Errorf("services = %v", sc.ServiceNames())
		}
		return nil
	})
	time.Sleep(1100 * time.Millisecond) // creation timestamps have second precision
	e.create(httpGateway("two", "second"), hostRoute("two", "r", "second", "shared", "web"))
	e.eventually("second reports the conflict", func() error {
		r := &gwv1.HTTPRoute{}
		if err := e.get("two", "r", r); err != nil {
			return err
		}
		if len(r.Status.Parents) == 0 {
			return fmt.Errorf("no parent status")
		}
		c := meta.FindStatusCondition(r.Status.Parents[0].Conditions, "Accepted")
		if c == nil || c.Reason != translate.ReasonConflicted || !strings.Contains(c.Message, "one/first") {
			return fmt.Errorf("accepted = %+v", c)
		}
		sc, err := e.serveConfig("two", "second")
		if err != nil {
			return err
		}
		if len(sc.Services) != 0 {
			return fmt.Errorf("second serves %v", sc.ServiceNames())
		}
		return nil
	})
	// Once the first Gateway is gone, the name moves to the second.
	if err := e.c.Delete(e.ctx, &gwv1.Gateway{ObjectMeta: metav1.ObjectMeta{Namespace: "one", Name: "first"}}); err != nil {
		t.Fatal(err)
	}
	e.eventually("second takes svc:shared over", func() error {
		// envtest runs no garbage collector: remove the owned ConfigMap.
		_ = e.c.Delete(e.ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "one", Name: "first"}})
		sc, err := e.serveConfig("two", "second")
		if err != nil {
			return err
		}
		if _, ok := sc.Services["svc:shared"]; !ok {
			return fmt.Errorf("services = %v", sc.ServiceNames())
		}
		return nil
	})

	// An IngressClass of another controller is not served, even under the
	// configured name.
	e.create(
		&networkingv1.IngressClass{ObjectMeta: metav1.ObjectMeta{Name: "tailnet"}, Spec: networkingv1.IngressClassSpec{Controller: "example.com/other"}},
		&networkingv1.Ingress{
			ObjectMeta: metav1.ObjectMeta{Namespace: "one", Name: "foreign"},
			Spec: networkingv1.IngressSpec{IngressClassName: ptr.To("tailnet"), DefaultBackend: &networkingv1.IngressBackend{
				Service: &networkingv1.IngressServiceBackend{Name: "web", Port: networkingv1.ServiceBackendPort{Number: 80}}}},
		})
	time.Sleep(2 * time.Second)
	if err := e.get("one", "foreign", &gwv1.Gateway{}); !apierrors.IsNotFound(err) {
		t.Errorf("Ingress of another controller was translated: %v", err)
	}

	// A LoadBalancer Service waits for its GatewayClass.
	e.create(&corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: "two", Name: "late", Annotations: map[string]string{config.AnnotationGatewayClass: "late"}},
		Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeLoadBalancer, LoadBalancerClass: ptr.To("tailnet"),
			Ports: []corev1.ServicePort{{Name: "pg", Port: 5432}}},
	})
	time.Sleep(time.Second)
	e.create(&gwv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: "late"}, Spec: gwv1.GatewayClassSpec{ControllerName: gwv1.GatewayController(config.DefaultControllerName)}})
	e.eventually("service exposed once its class exists", func() error {
		return e.get("two", "late", &gwv1.Gateway{})
	})
}
