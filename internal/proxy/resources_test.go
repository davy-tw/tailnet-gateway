package proxy

import (
	"os"
	"os/exec"
	"path/filepath"
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
	if pre := c.Lifecycle.PreStop.Exec.Command[2]; !strings.Contains(pre, "serve drain") || !strings.Contains(pre, "sleep 11\n") || !strings.Contains(pre, "sleep 14\n") {
		t.Errorf("preStop = %s", c.Lifecycle.PreStop.Exec.Command[2])
	}
	if psc := pod.SecurityContext; psc == nil || !*psc.RunAsNonRoot || *psc.RunAsUser == 0 || psc.SeccompProfile.Type != "RuntimeDefault" {
		t.Errorf("pod security context = %+v", psc)
	}
	if !strings.Contains(c.ReadinessProbe.Exec.Command[2], "serve status --json") {
		t.Errorf("readiness = %+v", c.ReadinessProbe)
	}
	if c.Lifecycle.PostStart != nil {
		t.Errorf("unexpected postStart hook: advertisement belongs to the readiness probe")
	}
	if env["TS_EXPERIMENTAL_SERVICE_AUTO_ADVERTISEMENT"] != "false" {
		t.Errorf("the image's auto-advertisement must be off")
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

func TestCertificateSharing(t *testing.T) {
	envOf := func(s *Spec) map[string]string {
		env := map[string]string{}
		for _, e := range s.StatefulSet().Spec.Template.Spec.Containers[0].Env {
			env[e.Name] = e.Value
		}
		return env
	}
	off := spec(2)
	off.CertDomains = []string{"web.tail1234.ts.net"} // ignored while off
	if _, ok := envOf(off)["TS_EXPERIMENTAL_CERT_SHARE"]; ok {
		t.Error("cert share env set while off")
	}
	if got := strings.Join(off.Role().Rules[0].ResourceNames, ","); got != "gw-0,gw-1" {
		t.Errorf("resourceNames while off = %s", got)
	}

	on := spec(2)
	on.Params.CertificateSharing = true
	on.CertDomains = []string{"web.tail1234.ts.net"}
	if envOf(on)["TS_EXPERIMENTAL_CERT_SHARE"] != "true" {
		t.Error("cert share env missing")
	}
	if got := strings.Join(on.Role().Rules[0].ResourceNames, ","); got != "gw-0,gw-1,web.tail1234.ts.net" {
		t.Errorf("resourceNames = %s", got)
	}
	for _, r := range on.Role().Rules {
		for _, v := range r.Verbs {
			if v == "list" || v == "watch" || (v == "create" && r.Resources[0] == "secrets") {
				t.Errorf("unexpected verb %s on %v", v, r.Resources)
			}
		}
	}
	if len(on.Role().Rules) != 2 {
		t.Errorf("rules with the restart reload = %+v", on.Role().Rules)
	}
	list := spec(2)
	list.Params.CertificateSharing = true
	list.Params.CertificateReload = config.CertificateReloadList
	if rules := list.Role().Rules; len(rules) != 3 || rules[2].Verbs[0] != "list" || len(rules[2].ResourceNames) != 0 || rules[2].Resources[0] != "secrets" {
		t.Errorf("rules with the list reload = %+v", rules)
	}
	off.Params.CertificateReload = config.CertificateReloadList // ignored while off
	if len(off.Role().Rules) != 2 {
		t.Errorf("list granted while sharing is off")
	}

	sec := on.CertSecret("web.tail1234.ts.net", map[string]string{AnnotationCertificateSHA256: "sum"})
	if sec.Name != "web.tail1234.ts.net" || sec.Namespace != "ns" || len(sec.Data) != 0 {
		t.Errorf("cert Secret = %+v", sec.ObjectMeta)
	}
	for k, v := range map[string]string{LabelTailscaleSecretType: "certs", LabelTailscaleManaged: "true",
		LabelTailscaleProxyGroup: "gw", LabelComponent: ComponentCertificate, LabelManagedBy: ManagedBy} {
		if sec.Labels[k] != v {
			t.Errorf("label %s = %q, want %q", k, sec.Labels[k], v)
		}
	}
	if sec.Annotations[AnnotationCertificateSHA256] != "sum" {
		t.Errorf("annotations = %v", sec.Annotations)
	}
}

// fakeTailscale is a stand-in for the tailscale CLI that keeps its state
// (running or not, the serve config it runs and the advertised Services) in
// files under dir. "debug prefs" prints indented JSON, as the real CLI does.
const fakeTailscale = `#!/bin/sh
D=$(dirname "$0")
shift # --socket
case "$1 $2" in
"status "*) [ -e "$D/up" ] ;;
"serve status") cat "$D/running.json" ;;
"serve advertise") [ -e "$D/stop-advertise" ] && exit 3; grep -qxF "$3" "$D/adv" 2>/dev/null || echo "$3" >> "$D/adv"; echo "advertise $3" >> "$D/log" ;;
"serve drain") grep -vxF "$3" "$D/adv" > "$D/adv.new" 2>/dev/null; mv "$D/adv.new" "$D/adv"; echo "drain $3" >> "$D/log"
	# A write lost to a concurrent probe: the first drain is undone.
	[ -e "$D/undo-drain" ] && { rm "$D/undo-drain"; echo "$3" >> "$D/adv"; } ;;
"debug prefs")
	# A hook run once, to act between the probe's steps.
	[ -x "$D/on-prefs" ] && { mv "$D/on-prefs" "$D/on-prefs.done"; "$D/on-prefs.done"; }
	[ -e "$D/old-prefs" ] && { printf '{\n\t"WantRunning": true\n}\n'; exit 0; }
	printf '{\n\t"WantRunning": true,\n\t"AdvertiseServices": '
	if [ -s "$D/adv" ]; then printf '[\n'; sed 's/.*/\t\t"&"/' "$D/adv" | paste -sd, -; printf '\t]'; else printf 'null'; fi
	printf ',\n\t"Hostname": "x"\n}\n' ;;
*) exit 2 ;;
esac
`

type proxySim struct {
	t   *testing.T
	dir string
}

func newProxySim(t *testing.T) *proxySim {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "tailscale"), []byte(fakeTailscale), 0o755); err != nil {
		t.Fatal(err)
	}
	return &proxySim{t: t, dir: dir}
}

func (p *proxySim) write(name, content string) {
	p.t.Helper()
	if err := os.WriteFile(filepath.Join(p.dir, name), []byte(content), 0o644); err != nil {
		p.t.Fatal(err)
	}
}

func (p *proxySim) read(name string) string {
	b, _ := os.ReadFile(filepath.Join(p.dir, name))
	return strings.TrimSpace(string(b))
}

// run runs a pod script against the fake CLI, with the mounted config and
// the draining marker moved into the test directory.
func (p *proxySim) run(script string) bool {
	p.t.Helper()
	script = strings.ReplaceAll(script, ServeConfigDir+"/"+ServeConfigKey, filepath.Join(p.dir, "mounted.json"))
	script = strings.ReplaceAll(script, DrainingMarker, filepath.Join(p.dir, "draining"))
	cmd := exec.Command("/bin/sh", "-c", script)
	cmd.Env = append(os.Environ(), "PATH="+p.dir+":"+os.Getenv("PATH"))
	out, err := cmd.CombinedOutput()
	if err != nil {
		if _, ok := err.(*exec.ExitError); !ok {
			p.t.Fatalf("%v: %s", err, out)
		}
		return false
	}
	return true
}

func serveJSON(svcs ...string) string {
	var b strings.Builder
	b.WriteString(`{"Services":{`)
	for i, s := range svcs {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(`"` + s + `":{"TCP":{"80":{"HTTP":true}}}`)
	}
	b.WriteString("}}")
	return b.String()
}

// The readiness probe advertises the Services of the applied config and
// drains the ones removed from it, so that a Gateway change takes effect
// without restarting the replicas.
func TestReadyScriptReconcilesAdvertisement(t *testing.T) {
	if _, err := exec.LookPath("paste"); err != nil {
		t.Skip("paste not available")
	}
	p := newProxySim(t)
	ready := ReadyScript()

	p.write("mounted.json", serveJSON("svc:a", "svc:b"))
	p.write("running.json", serveJSON("svc:a", "svc:b"))
	if p.run(ready) {
		t.Fatal("ready while tailscaled is down")
	}
	p.write("up", "")
	if !p.run(ready) {
		t.Fatalf("not ready; log:\n%s", p.read("log"))
	}
	if got := p.read("adv"); got != "svc:a\nsvc:b" {
		t.Fatalf("advertised = %q", got)
	}

	// A listener change: the mounted file changes before the image applies
	// it. Nothing is advertised until the config runs.
	p.write("mounted.json", serveJSON("svc:a", "svc:c"))
	if p.run(ready) {
		t.Fatal("ready before the new config runs")
	}
	if got := p.read("adv"); got != "svc:a\nsvc:b" {
		t.Fatalf("advertised before the config runs = %q", got)
	}
	p.write("running.json", serveJSON("svc:a", "svc:c"))
	if !p.run(ready) {
		t.Fatalf("not ready after the change; log:\n%s", p.read("log"))
	}
	if got := p.read("adv"); got != "svc:a\nsvc:c" {
		t.Fatalf("advertised after the change = %q", got)
	}

	// Steady state: no further CLI calls.
	p.write("log", "")
	if !p.run(ready) || p.read("log") != "" {
		t.Fatalf("steady state made changes: %q", p.read("log"))
	}

	// Draining: everything is withdrawn, and the probe neither reports
	// ready nor advertises again.
	if !p.run(DrainScript(0)) {
		t.Fatal("drain failed")
	}
	if got := p.read("adv"); got != "" {
		t.Fatalf("advertised after drain = %q", got)
	}
	if p.run(ready) || p.read("adv") != "" {
		t.Fatalf("probe acted while draining: ready, advertised %q", p.read("adv"))
	}
}

// The preStop race: a probe run that passed the marker check before preStop
// created it must not advertise again, and a drain lost to a concurrent
// probe write is repeated once the probe run is over.
func TestDrainRacesWithProbe(t *testing.T) {
	if _, err := exec.LookPath("paste"); err != nil {
		t.Skip("paste not available")
	}
	p := newProxySim(t)
	p.write("up", "")
	p.write("mounted.json", serveJSON("svc:a", "svc:b"))
	p.write("running.json", serveJSON("svc:a", "svc:b"))
	p.write("adv", "svc:a\n")

	// preStop runs while the probe is between its marker check and its
	// prefs read: the probe sees svc:b missing but must not advertise it.
	p.write("on-prefs", "#!/bin/sh\ntouch "+filepath.Join(p.dir, "draining")+"\n")
	if err := os.Chmod(filepath.Join(p.dir, "on-prefs"), 0o755); err != nil {
		t.Fatal(err)
	}
	if p.run(ReadyScript()) {
		t.Fatal("ready while draining")
	}
	if got := p.read("adv"); got != "svc:a" {
		t.Fatalf("probe advertised during drain: %q", got)
	}

	// The first drain's write is lost; the second one withdraws it.
	p.write("undo-drain", "")
	if !p.run(DrainScript(0)) {
		t.Fatal("drain failed")
	}
	if got := p.read("adv"); got != "" {
		t.Fatalf("still advertised after drain: %q", got)
	}
}

// Until the mounted config runs, stale advertisements (in neither the
// mounted nor the running config) are drained, and nothing is advertised.
func TestReadyScriptDrainsStaleWhileConfigPending(t *testing.T) {
	if _, err := exec.LookPath("paste"); err != nil {
		t.Skip("paste not available")
	}
	p := newProxySim(t)
	p.write("up", "")
	p.write("mounted.json", serveJSON("svc:a", "svc:new"))
	p.write("running.json", serveJSON("svc:a", "svc:old"))
	p.write("adv", "svc:a\nsvc:old\nsvc:gone\n")
	if p.run(ReadyScript()) {
		t.Fatal("ready before the config runs")
	}
	if got := p.read("adv"); got != "svc:a\nsvc:old" {
		t.Fatalf("advertised = %q, want only svc:gone drained", got)
	}
}

// An image whose prefs output has no AdvertiseServices field must not be
// read as "nothing advertised".
func TestReadyScriptNeedsAdvertiseServicesField(t *testing.T) {
	p := newProxySim(t)
	p.write("up", "")
	p.write("mounted.json", serveJSON("svc:a"))
	p.write("running.json", serveJSON("svc:a"))
	p.write("old-prefs", "")
	p.write("stop-advertise", "")
	if p.run(ReadyScript()) {
		t.Fatal("ready without AdvertiseServices")
	}
	if got := p.read("log"); got != "" {
		t.Fatalf("CLI changes made: %q", got)
	}
}
