//go:build e2e

// Package e2e runs against a real cluster (the current kubeconfig context)
// with the Gateway API CRDs and tailnet-gateway installed:
//
//	go test -tags e2e ./test/e2e/ -v
//
// Without a tailnet (the default) it checks what needs a real cluster
// rather than envtest: proxy pods admitted under the restricted Pod
// Security Standard and started by a real kubelet, Gateway, route,
// Ingress and Service translation, and Gateway deletion with real garbage
// collection (also foreground) and the GatewayClass finalizer.
//
// Settings (environment variables):
//
//   - E2E_GATEWAY_CLASS: the installed GatewayClass (default tailnet).
//   - E2E_CONTROLLER_NAME: its controllerName, for the classes the tests
//     create (default tailnet-gateway.davy.tw/gateway-controller).
//   - E2E_AUTHKEY: for a controller in manual key mode, the auth key put in
//     each test Gateway's Secret. Any value starts the proxies; a fake one
//     is enough unless E2E_EXPECT_PROGRAMMED is set.
//   - E2E_EXPECT_PROGRAMMED=1: a tailnet with OAuth or a real key, and the
//     Tailscale Service svc:<test namespace> approved. The smoke test then
//     also waits for the Gateway to be Programmed and to publish an address.
package e2e

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
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
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"
)

type suite struct {
	t   *testing.T
	ctx context.Context
	c   client.Client
	ns  string
}

func env(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

// setup connects to the cluster and creates a namespace for the test that
// enforces the restricted Pod Security Standard.
func setup(t *testing.T, prefix string) *suite {
	t.Helper()
	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(gwv1.Install(scheme))
	c, err := client.New(ctrl.GetConfigOrDie(), client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	s := &suite{t: t, ctx: context.Background(), c: c, ns: fmt.Sprintf("%s-%d", prefix, time.Now().Unix())}
	s.create(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: s.ns, Labels: map[string]string{
		"pod-security.kubernetes.io/enforce": "restricted",
	}}})
	t.Cleanup(func() {
		if t.Failed() {
			s.dump()
		}
		_ = c.Delete(context.Background(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: s.ns}})
	})
	return s
}

func (s *suite) create(objs ...client.Object) {
	s.t.Helper()
	for _, o := range objs {
		if err := s.c.Create(s.ctx, o); err != nil {
			s.t.Fatalf("creating %T %s: %v", o, o.GetName(), err)
		}
	}
}

func (s *suite) key(name string) types.NamespacedName {
	return types.NamespacedName{Namespace: s.ns, Name: name}
}

func (s *suite) wait(what string, timeout time.Duration, f func() error) {
	s.t.Helper()
	deadline := time.Now().Add(timeout)
	var err error
	for time.Now().Before(deadline) {
		if err = f(); err == nil {
			return
		}
		time.Sleep(2 * time.Second)
	}
	s.t.Fatalf("%s: %v", what, err)
}

func (s *suite) gone(obj client.Object, name string) func() error {
	return func() error {
		if err := s.c.Get(s.ctx, s.key(name), obj); !apierrors.IsNotFound(err) {
			return fmt.Errorf("%T %s still there (%v)", obj, name, err)
		}
		return nil
	}
}

// dump logs the test namespace's Gateways and pods to explain a failure.
func (s *suite) dump() {
	var gws gwv1.GatewayList
	if err := s.c.List(s.ctx, &gws, client.InNamespace(s.ns)); err == nil {
		for _, gw := range gws.Items {
			s.t.Logf("Gateway %s: finalizers=%v deleting=%v conditions=%+v", gw.Name, gw.Finalizers, gw.DeletionTimestamp != nil, gw.Status.Conditions)
		}
	}
	var pods corev1.PodList
	if err := s.c.List(s.ctx, &pods, client.InNamespace(s.ns)); err == nil {
		for _, p := range pods.Items {
			s.t.Logf("Pod %s: phase=%s containers=%+v", p.Name, p.Status.Phase, p.Status.ContainerStatuses)
		}
	}
	var evs corev1.EventList
	if err := s.c.List(s.ctx, &evs, client.InNamespace(s.ns)); err == nil {
		for _, e := range evs.Items {
			s.t.Logf("Event %s %s/%s: %s %s", e.Type, e.InvolvedObject.Kind, e.InvolvedObject.Name, e.Reason, e.Message)
		}
	}
}

// authKey creates the manual-mode auth key Secret for a Gateway, when
// E2E_AUTHKEY is set.
func (s *suite) authKey(gateway string) {
	if key := os.Getenv("E2E_AUTHKEY"); key != "" {
		s.create(&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: s.ns, Name: gateway}, StringData: map[string]string{"authkey": key}})
	}
}

func condTrue(conds []metav1.Condition, typ string) error {
	c := meta.FindStatusCondition(conds, typ)
	if c == nil || c.Status != metav1.ConditionTrue {
		return fmt.Errorf("%s not true: %+v", typ, c)
	}
	return nil
}

func (s *suite) gatewayAccepted(name string) func() error {
	return func() error {
		gw := &gwv1.Gateway{}
		if err := s.c.Get(s.ctx, s.key(name), gw); err != nil {
			return err
		}
		return condTrue(gw.Status.Conditions, "Accepted")
	}
}

// proxyStarted waits until a proxy pod of the Gateway was admitted and its
// container started at least once. Without a tailnet the container may
// then exit, since it cannot log in.
func (s *suite) proxyStarted(gateway string) func() error {
	return func() error {
		pod := &corev1.Pod{}
		if err := s.c.Get(s.ctx, s.key(gateway+"-0"), pod); err != nil {
			return err
		}
		for _, cs := range pod.Status.ContainerStatuses {
			if cs.State.Running != nil || cs.State.Terminated != nil || cs.LastTerminationState.Terminated != nil {
				return nil
			}
			if w := cs.State.Waiting; w != nil && w.Reason != "ContainerCreating" && w.Reason != "PodInitializing" && w.Reason != "CrashLoopBackOff" {
				return fmt.Errorf("container %s waiting: %s %s", cs.Name, w.Reason, w.Message)
			}
		}
		return fmt.Errorf("pod %s not started yet: %s", pod.Name, pod.Status.Phase)
	}
}

func httpGateway(ns, name, class string) *gwv1.Gateway {
	return &gwv1.Gateway{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec: gwv1.GatewaySpec{GatewayClassName: gwv1.ObjectName(class),
			Listeners: []gwv1.Listener{{Name: "http", Protocol: gwv1.HTTPProtocolType, Port: 80}}}}
}

func backend(ns string) *corev1.Service {
	return &corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "web"},
		Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Name: "http", Port: 80}}}}
}

func TestGatewaySmoke(t *testing.T) {
	s := setup(t, "tailnet-e2e")
	class := env("E2E_GATEWAY_CLASS", "tailnet")
	svcLabel := s.ns // unique Tailscale Service name per run
	s.authKey("e2e")
	s.create(backend(s.ns), httpGateway(s.ns, "e2e", class),
		&gwv1.HTTPRoute{ObjectMeta: metav1.ObjectMeta{Namespace: s.ns, Name: "e2e"},
			Spec: gwv1.HTTPRouteSpec{
				CommonRouteSpec: gwv1.CommonRouteSpec{ParentRefs: []gwv1.ParentReference{{Name: "e2e"}}},
				Hostnames:       []gwv1.Hostname{gwv1.Hostname(svcLabel)},
				Rules: []gwv1.HTTPRouteRule{{BackendRefs: []gwv1.HTTPBackendRef{{BackendRef: gwv1.BackendRef{
					BackendObjectReference: gwv1.BackendObjectReference{Name: "web", Port: ptr.To(gwv1.PortNumber(80))}}}}}},
			}})

	s.wait("Gateway accepted", time.Minute, s.gatewayAccepted("e2e"))
	s.wait("HTTPRoute accepted", time.Minute, func() error {
		r := &gwv1.HTTPRoute{}
		if err := s.c.Get(s.ctx, s.key("e2e"), r); err != nil {
			return err
		}
		if len(r.Status.Parents) == 0 {
			return fmt.Errorf("no parent status")
		}
		return condTrue(r.Status.Parents[0].Conditions, "Accepted")
	})
	s.wait("StatefulSet created", time.Minute, func() error {
		return s.c.Get(s.ctx, s.key("e2e"), &appsv1.StatefulSet{})
	})
	s.wait("serve config written", time.Minute, func() error {
		cm := &corev1.ConfigMap{}
		if err := s.c.Get(s.ctx, s.key("e2e"), cm); err != nil {
			return err
		}
		if cm.Annotations["tailnet-gateway.davy.tw/tailscale-services"] != svcLabel {
			return fmt.Errorf("services annotation = %q", cm.Annotations["tailnet-gateway.davy.tw/tailscale-services"])
		}
		return nil
	})
	s.wait("proxy started under the restricted Pod Security Standard", 3*time.Minute, s.proxyStarted("e2e"))

	if os.Getenv("E2E_EXPECT_PROGRAMMED") != "1" {
		return
	}
	s.wait("Gateway programmed", 5*time.Minute, func() error {
		gw := &gwv1.Gateway{}
		if err := s.c.Get(s.ctx, s.key("e2e"), gw); err != nil {
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

// TestListenerSet attaches a ListenerSet to a Gateway that allows
// ListenerSets from its own namespace, routes through it, and checks that
// its listener is served by the Gateway's proxy.
func TestListenerSet(t *testing.T) {
	s := setup(t, "tailnet-e2e-ls")
	class := env("E2E_GATEWAY_CLASS", "tailnet")
	svcLabel := s.ns
	gw := httpGateway(s.ns, "shared", class)
	gw.Spec.AllowedListeners = &gwv1.AllowedListeners{Namespaces: &gwv1.ListenerNamespaces{From: ptr.To(gwv1.NamespacesFromSame)}}
	s.authKey("shared")
	s.create(backend(s.ns), gw,
		&gwv1.ListenerSet{ObjectMeta: metav1.ObjectMeta{Namespace: s.ns, Name: "apps"},
			Spec: gwv1.ListenerSetSpec{
				ParentRef: gwv1.ParentGatewayReference{Name: "shared"},
				Listeners: []gwv1.ListenerEntry{{Name: "web", Protocol: gwv1.HTTPProtocolType, Port: 8080}},
			}},
		&gwv1.HTTPRoute{ObjectMeta: metav1.ObjectMeta{Namespace: s.ns, Name: "via-set"},
			Spec: gwv1.HTTPRouteSpec{
				CommonRouteSpec: gwv1.CommonRouteSpec{ParentRefs: []gwv1.ParentReference{
					{Kind: ptr.To(gwv1.Kind("ListenerSet")), Name: "apps", SectionName: ptr.To(gwv1.SectionName("web"))}}},
				Hostnames: []gwv1.Hostname{gwv1.Hostname(svcLabel)},
				Rules: []gwv1.HTTPRouteRule{{BackendRefs: []gwv1.HTTPBackendRef{{BackendRef: gwv1.BackendRef{
					BackendObjectReference: gwv1.BackendObjectReference{Name: "web", Port: ptr.To(gwv1.PortNumber(80))}}}}}},
			}})

	s.wait("Gateway accepted", time.Minute, s.gatewayAccepted("shared"))
	s.wait("ListenerSet accepted", time.Minute, func() error {
		ls := &gwv1.ListenerSet{}
		if err := s.c.Get(s.ctx, s.key("apps"), ls); err != nil {
			return err
		}
		if err := condTrue(ls.Status.Conditions, "Accepted"); err != nil {
			return err
		}
		if len(ls.Status.Listeners) != 1 || ls.Status.Listeners[0].AttachedRoutes != 1 {
			return fmt.Errorf("listeners = %+v", ls.Status.Listeners)
		}
		return nil
	})
	s.wait("ListenerSet counted on the Gateway", time.Minute, func() error {
		gw := &gwv1.Gateway{}
		if err := s.c.Get(s.ctx, s.key("shared"), gw); err != nil {
			return err
		}
		if n := ptr.Deref(gw.Status.AttachedListenerSets, -1); n != 1 {
			return fmt.Errorf("attachedListenerSets = %d", n)
		}
		return nil
	})
	s.wait("HTTPRoute accepted by the ListenerSet", time.Minute, func() error {
		r := &gwv1.HTTPRoute{}
		if err := s.c.Get(s.ctx, s.key("via-set"), r); err != nil {
			return err
		}
		if len(r.Status.Parents) != 1 || r.Status.Parents[0].ParentRef.Kind == nil || *r.Status.Parents[0].ParentRef.Kind != "ListenerSet" {
			return fmt.Errorf("parents = %+v", r.Status.Parents)
		}
		return condTrue(r.Status.Parents[0].Conditions, "Accepted")
	})
	s.wait("served by the Gateway's proxy", time.Minute, func() error {
		cm := &corev1.ConfigMap{}
		if err := s.c.Get(s.ctx, s.key("shared"), cm); err != nil {
			return err
		}
		if got := cm.Annotations["tailnet-gateway.davy.tw/tailscale-services"]; got != svcLabel {
			return fmt.Errorf("services annotation = %q", got)
		}
		return nil
	})
	s.wait("proxy started", 3*time.Minute, s.proxyStarted("shared"))
}

// TestDeletion deletes a Gateway whose proxy runs, in the foreground, and
// checks that the controller's finalizer and garbage collection remove
// everything, and that its GatewayClass is kept until then.
func TestDeletion(t *testing.T) {
	s := setup(t, "tailnet-e2e-del")
	classes := &gwv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: s.ns},
		Spec: gwv1.GatewayClassSpec{ControllerName: gwv1.GatewayController(env("E2E_CONTROLLER_NAME", "tailnet-gateway.davy.tw/gateway-controller"))}}
	s.create(classes)
	t.Cleanup(func() { _ = s.c.Delete(context.Background(), classes) })
	s.authKey("del")
	s.create(backend(s.ns), httpGateway(s.ns, "del", s.ns))
	s.wait("Gateway accepted", time.Minute, s.gatewayAccepted("del"))
	s.wait("proxy started", 3*time.Minute, s.proxyStarted("del"))

	// The class is in use: deleting it leaves it in place.
	s.wait("class protected", time.Minute, func() error {
		gc := &gwv1.GatewayClass{}
		if err := s.c.Get(s.ctx, types.NamespacedName{Name: s.ns}, gc); err != nil {
			return err
		}
		for _, f := range gc.Finalizers {
			if f == gwv1.GatewayClassFinalizerGatewaysExist {
				return nil
			}
		}
		return fmt.Errorf("finalizers = %v", gc.Finalizers)
	})
	if err := s.c.Delete(s.ctx, classes); err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Second)
	if err := s.c.Get(s.ctx, types.NamespacedName{Name: s.ns}, &gwv1.GatewayClass{}); err != nil {
		t.Fatalf("class in use was deleted: %v", err)
	}
	s.wait("Gateway still served while its class is being deleted", time.Minute, s.gatewayAccepted("del"))

	gw := &gwv1.Gateway{}
	if err := s.c.Get(s.ctx, s.key("del"), gw); err != nil {
		t.Fatal(err)
	}
	if err := s.c.Delete(s.ctx, gw, client.PropagationPolicy(metav1.DeletePropagationForeground)); err != nil {
		t.Fatal(err)
	}
	s.wait("Gateway deleted", 4*time.Minute, s.gone(&gwv1.Gateway{}, "del"))
	s.wait("StatefulSet deleted", time.Minute, s.gone(&appsv1.StatefulSet{}, "del"))
	s.wait("proxy pod deleted", time.Minute, s.gone(&corev1.Pod{}, "del-0"))
	s.wait("state Secret deleted", time.Minute, s.gone(&corev1.Secret{}, "del-0"))
	s.wait("serve config deleted", time.Minute, s.gone(&corev1.ConfigMap{}, "del"))
	s.wait("class released and deleted", time.Minute, func() error {
		if err := s.c.Get(s.ctx, types.NamespacedName{Name: s.ns}, &gwv1.GatewayClass{}); !apierrors.IsNotFound(err) {
			return fmt.Errorf("class still there (%v)", err)
		}
		return nil
	})
}

// TestIngressAndService checks that an Ingress and a LoadBalancer Service
// get a Gateway of the default class, and that the Service's proxy runs.
func TestIngressAndService(t *testing.T) {
	s := setup(t, "tailnet-e2e-gen")
	ingressClass := env("E2E_INGRESS_CLASS", "tailnet")
	s.authKey("site")
	s.authKey("db")
	s.create(backend(s.ns),
		&networkingv1.Ingress{ObjectMeta: metav1.ObjectMeta{Namespace: s.ns, Name: "site"},
			Spec: networkingv1.IngressSpec{IngressClassName: ptr.To(ingressClass), DefaultBackend: &networkingv1.IngressBackend{
				Service: &networkingv1.IngressServiceBackend{Name: "web", Port: networkingv1.ServiceBackendPort{Number: 80}}}}},
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: s.ns, Name: "db"},
			Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeLoadBalancer, LoadBalancerClass: ptr.To(env("E2E_LOAD_BALANCER_CLASS", "tailnet")),
				Ports: []corev1.ServicePort{{Name: "pg", Port: 5432}}}},
	)
	s.wait("Ingress Gateway accepted", time.Minute, s.gatewayAccepted("site"))
	s.wait("Service Gateway accepted", time.Minute, s.gatewayAccepted("db"))
	s.wait("Service proxy started", 3*time.Minute, s.proxyStarted("db"))
}
