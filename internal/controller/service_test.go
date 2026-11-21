package controller

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"go.pkgs.davy.tw/tailnet-gateway/internal/config"
)

func TestPlanService(t *testing.T) {
	ports := []corev1.ServicePort{
		{Name: "ssh", Port: 22},
		{Name: "web", Port: 80, AppProtocol: ptr.To("http")},
		{Name: "ws", Port: 8080, AppProtocol: ptr.To("kubernetes.io/ws")},
		{Name: "dns", Port: 53, Protocol: corev1.ProtocolUDP},
	}
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "app"}, Spec: corev1.ServiceSpec{Ports: ports}}

	plan := PlanService(svc)
	if plan.HTTP != nil || len(plan.TCP) != 3 || len(plan.Warnings) != 1 || plan.Label != "app" {
		t.Errorf("without hostname: %+v", plan)
	}

	svc.Annotations = map[string]string{config.AnnotationHostname: "web"}
	plan = PlanService(svc)
	if plan.HTTP == nil || plan.HTTP.Port != 80 || plan.Label != "web" {
		t.Errorf("http port: %+v", plan)
	}
	if len(plan.TCP) != 2 || plan.TCP[0].Port != 22 || plan.TCP[1].Port != 8080 {
		t.Errorf("tcp ports: %+v (only the selected HTTP port leaves the TCP forwards)", plan.TCP)
	}

	// TLS and h2c backends are forwarded as raw TCP, never served as HTTP.
	svc.Spec.Ports = []corev1.ServicePort{
		{Name: "tls", Port: 8443, AppProtocol: ptr.To("https")},
		{Name: "grpc", Port: 9090, AppProtocol: ptr.To("kubernetes.io/h2c")},
		{Name: "plain", Port: 3000},
	}
	if plan = PlanService(svc); plan.HTTP == nil || plan.HTTP.Port != 3000 || len(plan.TCP) != 2 {
		t.Errorf("raw protocols: %+v", plan)
	}
	svc.Spec.Ports = svc.Spec.Ports[:2]
	if plan = PlanService(svc); plan.HTTP != nil || len(plan.TCP) != 2 {
		t.Errorf("only raw protocols: %+v", plan)
	}

	svc.Spec.Ports = []corev1.ServicePort{{Name: "a", Port: 3000}, {Name: "b", Port: 22}}
	if plan = PlanService(svc); plan.HTTP == nil || plan.HTTP.Port != 3000 || len(plan.TCP) != 1 {
		t.Errorf("fallback: %+v", plan)
	}
}

func TestPlanServiceHostname(t *testing.T) {
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "app", Annotations: map[string]string{config.AnnotationHostname: "db.tail1234.ts.net"}},
		Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Name: "pg", Port: 5432, AppProtocol: ptr.To("https")}}}}
	if plan := PlanService(svc); plan.Label != "db" || plan.Invalid != "" {
		t.Errorf("tailnet name: %+v", plan)
	}
	svc.Annotations[config.AnnotationHostname] = "bad_host"
	if plan := PlanService(svc); plan.Invalid == "" {
		t.Errorf("invalid hostname accepted: %+v", plan)
	}
}

func TestDomainFromFQDN(t *testing.T) {
	for in, want := range map[string]string{
		"gw-0.tail1234.ts.net.": "tail1234.ts.net",
		"gw-0.Tail1234.ts.net":  "tail1234.ts.net",
		"gw-0":                  "",
		"gw-0.local":            "",
	} {
		if got := DomainFromFQDN(in); got != want {
			t.Errorf("DomainFromFQDN(%q) = %q, want %q", in, got, want)
		}
	}
}
