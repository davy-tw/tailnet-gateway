package proxy

import (
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"go.pkgs.davy.tw/tailnet-gateway/internal/config"
)

func spec(replicas int32) *Spec {
	return &Spec{
		Namespace: "ns", Gateway: "gw",
		Params:    config.ClassParams{Replicas: replicas, Image: "img", DrainGracePeriod: 30 * time.Second},
		PodLabels: map[string]string{"team": "a", LabelInstance: "spoofed"},
		Owner:     metav1.OwnerReference{Name: "gw"},
	}
}

func TestStatefulSet(t *testing.T) {
	sts := spec(3).StatefulSet()
	if *sts.Spec.Replicas != 3 || sts.Spec.PodManagementPolicy != "Parallel" {
		t.Errorf("spec = %+v", sts.Spec)
	}
	if got := sts.Spec.Template.Labels; got["team"] != "a" || got[LabelInstance] != "gw" {
		t.Errorf("pod labels = %v", got)
	}
	if got := sts.Spec.Selector.MatchLabels; len(got) != 2 {
		t.Errorf("selector = %v", got)
	}
	pod := sts.Spec.Template.Spec
	if *pod.TerminationGracePeriodSeconds != 30 {
		t.Errorf("grace = %d", *pod.TerminationGracePeriodSeconds)
	}
	c := pod.Containers[0]
	env := map[string]string{}
	for _, e := range c.Env {
		env[e.Name] = e.Value
	}
	for k, v := range map[string]string{
		"TS_USERSPACE": "true", "TS_KUBE_SECRET": "$(POD_NAME)", "TS_HOSTNAME": "$(POD_NAMESPACE)-$(POD_NAME)",
		"TS_SERVE_CONFIG": "/etc/tailnet-gateway/serve.json",
	} {
		if env[k] != v {
			t.Errorf("env %s = %q, want %q", k, env[k], v)
		}
	}
	if c.SecurityContext.Capabilities.Add != nil || c.SecurityContext.Capabilities.Drop[0] != "ALL" {
		t.Errorf("capabilities = %+v", c.SecurityContext.Capabilities)
	}
	if !strings.Contains(c.Lifecycle.PreStop.Exec.Command[2], "serve drain") || !strings.Contains(c.Lifecycle.PreStop.Exec.Command[2], "sleep 25") {
		t.Errorf("preStop = %s", c.Lifecycle.PreStop.Exec.Command[2])
	}
	if psc := pod.SecurityContext; psc == nil || !*psc.RunAsNonRoot || *psc.RunAsUser == 0 || psc.SeccompProfile.Type != "RuntimeDefault" {
		t.Errorf("pod security context = %+v", psc)
	}
	if !strings.Contains(c.ReadinessProbe.Exec.Command[2], "serve status --json") {
		t.Errorf("readiness = %+v", c.ReadinessProbe)
	}
	if !strings.Contains(c.Lifecycle.PostStart.Exec.Command[2], "serve advertise") {
		t.Errorf("postStart = %s", c.Lifecycle.PostStart.Exec.Command[2])
	}
}

func TestRoleAndPDB(t *testing.T) {
	r := spec(3).Role()
	if got := strings.Join(r.Rules[0].ResourceNames, ","); got != "gw-0,gw-1,gw-2" {
		t.Errorf("resourceNames = %s", got)
	}
	if spec(1).PodDisruptionBudget() != nil {
		t.Error("no PDB expected for one replica")
	}
	if pdb := spec(2).PodDisruptionBudget(); pdb.Spec.MaxUnavailable.IntValue() != 1 {
		t.Errorf("pdb = %+v", pdb.Spec)
	}
}
