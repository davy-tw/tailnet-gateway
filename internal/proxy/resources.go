// Package proxy builds the Kubernetes objects that make up a Gateway's
// data plane: a StatefulSet of Tailscale containers serving the generated
// serve config, with per-replica state Secrets and, when certificate
// sharing is on, one certificate Secret per TLS server name.
package proxy

import (
	"fmt"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	"go.pkgs.davy.tw/tailnet-gateway/internal/config"
)

// Labels and file locations.
const (
	LabelName      = "app.kubernetes.io/name"
	LabelInstance  = "app.kubernetes.io/instance"
	LabelManagedBy = "app.kubernetes.io/managed-by"
	LabelComponent = "app.kubernetes.io/component"
	LabelGateway   = "gateway.networking.k8s.io/gateway-name"

	AppName              = "tailnet-gateway-proxy"
	ManagedBy            = "tailnet-gateway"
	ComponentState       = "state"
	ComponentCertificate = "certificate"

	ServeConfigKey = "serve.json"
	ServeConfigDir = "/etc/tailnet-gateway"
	SocketPath     = "/tmp/tailscaled.sock"
	// ProxyUID runs the proxy containers as a non-root user. tailscaled
	// keeps its state in the state Secret and its files under /tmp.
	ProxyUID int64 = 1000

	// Keys the Tailscale container reads and writes in its state Secret.
	KeyAuthKey    = "authkey"
	KeyDeviceID   = "device_id"
	KeyDeviceFQDN = "device_fqdn"
)

// Labels that Tailscale's cert share mode looks for on certificate
// Secrets (kubestore's certSecretSelector). The proxies are not allowed to
// list Secrets, so they only matter if that changes.
const (
	LabelTailscaleSecretType = "tailscale.com/secret-type"
	LabelTailscaleManaged    = "tailscale.com/managed"
	LabelTailscaleProxyGroup = "tailscale.com/proxy-group"
)

// Annotations on a certificate Secret, kept by the controller: the
// SHA-256 and validity of the certificate it last saw there, and by when
// the reading replicas started before it must be restarted. Those keep the
// certificate they loaded until restarted, so after a renewal they are
// restarted before an older one expires.
const (
	AnnotationCertificateSHA256    = config.Domain + "/certificate-sha256"
	AnnotationCertificateNotBefore = config.Domain + "/certificate-not-before"
	AnnotationCertificateNotAfter  = config.Domain + "/certificate-not-after"
	AnnotationRestartReadersBy     = config.Domain + "/restart-readers-by"
)

// CertSecretLabels are the labels of a Gateway's certificate Secrets.
func CertSecretLabels(gateway string) map[string]string {
	l := Labels(gateway)
	l[LabelComponent] = ComponentCertificate
	l[LabelTailscaleSecretType] = "certs"
	l[LabelTailscaleManaged] = "true"
	l[LabelTailscaleProxyGroup] = gateway
	return l
}

// EnvCertShare turns on Tailscale's cert share mode in the proxy image.
const EnvCertShare = "TS_EXPERIMENTAL_CERT_SHARE"

// StateSecretName is the state Secret of replica i.
func StateSecretName(gateway string, i int) string { return fmt.Sprintf("%s-%d", gateway, i) }

// SelectorLabels select the proxy pods of a Gateway. They never change.
func SelectorLabels(gateway string) map[string]string {
	return map[string]string{LabelName: AppName, LabelInstance: gateway}
}

// Labels are put on every object created for a Gateway.
func Labels(gateway string) map[string]string {
	l := SelectorLabels(gateway)
	l[LabelManagedBy] = ManagedBy
	l[LabelGateway] = gateway
	return l
}

// Spec is everything needed to render a Gateway's data plane.
type Spec struct {
	Namespace string
	Gateway   string
	Params    config.ClassParams
	// PodLabels are extra labels copied from the Gateway onto pods.
	PodLabels map[string]string
	Owner     metav1.OwnerReference
	// CertDomains are the TLS server names whose certificates replica 0
	// issues for the others (Params.CertificateSharing only).
	CertDomains []string
	// CertGrants are the certificate Secrets the proxies may read and
	// write: CertDomains, and for a while names that are no longer served
	// (see the controller).
	CertGrants []string
}

// ListSecrets reports whether the proxies may list the Secrets of their
// namespace (certificate sharing with the "list" reload).
func (s *Spec) ListSecrets() bool {
	return s.Params.CertificateSharing && s.Params.CertificateReload == config.CertificateReloadList
}

// CertSecret is the Secret that Tailscale's cert share mode keeps the
// certificate for domain in. tailscaled names it after the domain and
// writes its tls.crt and tls.key; it is created empty, since the proxies
// may not create Secrets. annotations are the controller's bookkeeping.
func (s *Spec) CertSecret(domain string, annotations map[string]string) *corev1.Secret {
	m := s.meta(domain)
	m.Labels = CertSecretLabels(s.Gateway)
	if len(annotations) > 0 {
		m.Annotations = annotations
	}
	return &corev1.Secret{ObjectMeta: m, Type: corev1.SecretTypeOpaque}
}

func (s *Spec) meta(name string) metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Name:            name,
		Namespace:       s.Namespace,
		Labels:          Labels(s.Gateway),
		OwnerReferences: []metav1.OwnerReference{s.Owner},
	}
}

// ServiceAccount for the proxy pods.
func (s *Spec) ServiceAccount() *corev1.ServiceAccount {
	return &corev1.ServiceAccount{ObjectMeta: s.meta(s.Gateway)}
}

// Role lets the proxy pods read and update only their own state Secrets
// and, with certificate sharing, the Gateway's certificate Secrets. All
// replicas share one ServiceAccount, so the RBAC cannot tell replica 0
// from the others: only tailscaled's read-only mode keeps the others from
// writing. List is only given with the "list" reload, since it cannot be
// limited to these Secrets.
func (s *Spec) Role() *rbacv1.Role {
	names := make([]string, 0, int(s.Params.Replicas)+len(s.CertGrants))
	for i := 0; i < int(s.Params.Replicas); i++ {
		names = append(names, StateSecretName(s.Gateway, i))
	}
	names = append(names, s.CertGrants...)
	rules := []rbacv1.PolicyRule{
		{APIGroups: []string{""}, Resources: []string{"secrets"}, ResourceNames: names, Verbs: []string{"get", "update", "patch"}},
		{APIGroups: []string{""}, Resources: []string{"events"}, Verbs: []string{"create", "patch"}},
	}
	if s.ListSecrets() {
		rules = append(rules, rbacv1.PolicyRule{APIGroups: []string{""}, Resources: []string{"secrets"}, Verbs: []string{"list"}})
	}
	return &rbacv1.Role{ObjectMeta: s.meta(s.Gateway), Rules: rules}
}

// RoleBinding binds Role to ServiceAccount.
func (s *Spec) RoleBinding() *rbacv1.RoleBinding {
	return &rbacv1.RoleBinding{
		ObjectMeta: s.meta(s.Gateway),
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: s.Gateway},
		Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: s.Gateway, Namespace: s.Namespace}},
	}
}

// ConfigMap holds the serve config.
func (s *Spec) ConfigMap(serveConfig []byte) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: s.meta(s.Gateway),
		Data:       map[string]string{ServeConfigKey: string(serveConfig)},
	}
}

// PodDisruptionBudget allows one voluntary eviction at a time. It returns
// nil for a single replica, where it could only block drains.
func (s *Spec) PodDisruptionBudget() *policyv1.PodDisruptionBudget {
	if s.Params.Replicas < 2 {
		return nil
	}
	return &policyv1.PodDisruptionBudget{
		ObjectMeta: s.meta(s.Gateway),
		Spec: policyv1.PodDisruptionBudgetSpec{
			MaxUnavailable: ptr.To(intstr.FromInt32(1)),
			Selector:       &metav1.LabelSelector{MatchLabels: SelectorLabels(s.Gateway)},
		},
	}
}

func shellQuote(s string) string { return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'" }

// DrainingMarker is created by the preStop hook, so that the readiness
// probe stops advertising Services while the replica drains.
const DrainingMarker = "/tmp/tailnet-gateway-draining"

// ProbeTimeoutSeconds bounds one readiness probe run. A run that adds
// Services starts one CLI process per Service, so it gets more time than a
// plain check would need.
const ProbeTimeoutSeconds = 10

// scriptPrelude defines the variables and helpers shared by the pod
// scripts:
//   - svcs: the sorted "svc:" names in its input;
//   - has LIST NAME: whether the newline-separated LIST holds NAME;
//   - advertised: the names in the AdvertiseServices prefs, or failure
//     (with a message on stderr) when the prefs cannot be read or have no
//     such field. "tailscale debug prefs" is a debug command, so its
//     output (indented JSON in v1.104.7) is not a stable interface: a
//     missing field must not be read as "nothing advertised".
var scriptPrelude = []string{
	"CFG=" + shellQuote(ServeConfigDir+"/"+ServeConfigKey),
	"TS=" + shellQuote("tailscale --socket="+SocketPath),
	"DRAINING=" + shellQuote(DrainingMarker),
	`svcs() { grep -o '"svc:[a-z0-9-]*"' | tr -d '"' | sort -u; }`,
	`has() { printf '%s\n' "$1" | grep -qxF "$2"; }`,
	`advertised() {
  p=$($TS debug prefs 2>/dev/null | tr -d ' \t\n')
  case "$p" in *'"AdvertiseServices":'*) ;; *) echo "no AdvertiseServices field in tailscale debug prefs" >&2; return 1 ;; esac
  printf '%s' "$p" | grep -o '"AdvertiseServices":\[[^]]*\]' | svcs
}`,
}

// ReadyScript reports a replica ready once tailscaled is up, runs the
// mounted serve config and advertises exactly the Tailscale Services in it.
//
// The serve configs are compared by their strings (keys, hostnames, paths
// and targets), which catches any change of backend, path, port or mode;
// the mounted file is written with the same field names. An IP alone does
// not mean the replica serves the current config.
//
// Advertisement is reconciled here too. The image only advertises the
// Services of the config it starts with, so without this a Gateway change
// that adds a Service would never be advertised.
//   - Until the mounted config runs, only names in neither the mounted nor
//     the running config are drained (stale ones, for example kept in the
//     state Secret by a replica that was killed without its preStop hook).
//   - Once it runs, the config's Services that are missing are advertised
//     and the advertised ones no longer in it are drained.
//
// A draining replica (see DrainingMarker) is not ready and advertises
// nothing; the marker is checked again right before each advertise.
func ReadyScript() string {
	return strings.Join(append(append([]string(nil), scriptPrelude...),
		`[ -e "$DRAINING" ] && exit 1`,
		`$TS status >/dev/null 2>&1 || exit 1`,
		`running=$($TS serve status --json 2>/dev/null)`,
		`adv=$(advertised) || exit 1`,
		`mounted=$(svcs < "$CFG")`,
		`if [ "$(grep -o '"[^"]*"' "$CFG" | sort -u)" != "$(printf '%s' "$running" | grep -o '"[^"]*"' | sort -u)" ]; then`,
		`  live=$(printf '%s' "$running" | svcs)`,
		`  for s in $adv; do has "$mounted" "$s" || has "$live" "$s" || $TS serve drain "$s"; done`,
		`  exit 1`,
		`fi`,
		`[ "$mounted" = "$adv" ] && exit 0`,
		`for s in $mounted; do has "$adv" "$s" && continue; [ -e "$DRAINING" ] && exit 1; $TS serve advertise "$s"; done`,
		`for s in $adv; do has "$mounted" "$s" || $TS serve drain "$s"; done`,
		`[ "$mounted" = "$(advertised)" ]`,
	), "\n")
}

// DrainScript stops advertising every Tailscale Service, then waits so that
// open connections can finish before the container is stopped.
//
// A readiness probe that started before the marker was created may still
// advertise a Service, or lose the drain's write (the CLI edits the whole
// AdvertiseServices list). So the script drains, waits for any such probe
// run to end (ProbeTimeoutSeconds), and drains again.
func DrainScript(waitSeconds int64) string {
	settle := int64(ProbeTimeoutSeconds + 1)
	if settle > waitSeconds {
		settle = waitSeconds
	}
	drain := `for s in $( (svcs < "$CFG"; advertised) | sort -u); do $TS serve drain "$s" || true; done`
	return strings.Join(append(append([]string(nil), scriptPrelude...),
		`touch "$DRAINING"`,
		drain,
		fmt.Sprintf("sleep %d", settle),
		drain,
		fmt.Sprintf("sleep %d", waitSeconds-settle),
		`exit 0`,
	), "\n")
}

// StatefulSet runs the proxy replicas.
func (s *Spec) StatefulSet() *appsv1.StatefulSet {
	grace := int64(s.Params.DrainGracePeriod.Seconds())
	wait := grace - 5
	if wait < 0 {
		wait = 0
	}
	podLabels := map[string]string{}
	for k, v := range s.PodLabels {
		podLabels[k] = v
	}
	for k, v := range Labels(s.Gateway) {
		podLabels[k] = v
	}
	fieldEnv := func(name, path string) corev1.EnvVar {
		return corev1.EnvVar{Name: name, ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: path}}}
	}
	env := []corev1.EnvVar{
		fieldEnv("POD_NAME", "metadata.name"),
		fieldEnv("POD_NAMESPACE", "metadata.namespace"),
		fieldEnv("POD_UID", "metadata.uid"),
		fieldEnv("POD_IP", "status.podIP"),
		{Name: "TS_USERSPACE", Value: "true"},
		{Name: "TS_KUBE_SECRET", Value: "$(POD_NAME)"},
		{Name: "TS_HOSTNAME", Value: "$(POD_NAMESPACE)-$(POD_NAME)"},
		{Name: "TS_SERVE_CONFIG", Value: ServeConfigDir + "/" + ServeConfigKey},
		{Name: "TS_SOCKET", Value: SocketPath},
		{Name: "TS_AUTH_ONCE", Value: "true"},
		// The readiness probe owns advertisement (see ReadyScript). The
		// image's own would only follow the config it starts with, and
		// could advertise again while the preStop hook drains.
		{Name: "TS_EXPERIMENTAL_SERVICE_AUTO_ADVERTISEMENT", Value: "false"},
	}
	if s.Params.CertificateSharing {
		// The image's entrypoint makes the pod whose name ends in "-0"
		// the only one that issues and renews certificates ("rw"); the
		// others only read them ("ro").
		env = append(env, corev1.EnvVar{Name: EnvCertShare, Value: "true"})
	}
	selector := &metav1.LabelSelector{MatchLabels: SelectorLabels(s.Gateway)}
	return &appsv1.StatefulSet{
		ObjectMeta: s.meta(s.Gateway),
		Spec: appsv1.StatefulSetSpec{
			Replicas:            ptr.To(s.Params.Replicas),
			Selector:            selector,
			PodManagementPolicy: appsv1.ParallelPodManagement,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: podLabels},
				Spec: corev1.PodSpec{
					ServiceAccountName: s.Gateway,
					// Userspace networking needs no privileges, so the pods
					// meet the "restricted" Pod Security Standard.
					SecurityContext: &corev1.PodSecurityContext{
						RunAsNonRoot:   ptr.To(true),
						RunAsUser:      ptr.To(ProxyUID),
						RunAsGroup:     ptr.To(ProxyUID),
						SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
					},
					TerminationGracePeriodSeconds: ptr.To(grace),
					Affinity: &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{
						PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{{
							Weight: 100,
							PodAffinityTerm: corev1.PodAffinityTerm{
								LabelSelector: selector,
								TopologyKey:   "kubernetes.io/hostname",
							},
						}},
					}},
					TopologySpreadConstraints: []corev1.TopologySpreadConstraint{{
						MaxSkew:           1,
						TopologyKey:       "topology.kubernetes.io/zone",
						WhenUnsatisfiable: corev1.ScheduleAnyway,
						LabelSelector:     selector,
					}},
					Containers: []corev1.Container{{
						Name:  "tailscale",
						Image: s.Params.Image,
						Env:   env,
						VolumeMounts: []corev1.VolumeMount{{
							Name: "serve-config", MountPath: ServeConfigDir, ReadOnly: true,
						}},
						ReadinessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{
								Command: []string{"/bin/sh", "-c", ReadyScript()},
							}},
							PeriodSeconds:    5,
							TimeoutSeconds:   ProbeTimeoutSeconds,
							FailureThreshold: 2,
						},
						Lifecycle: &corev1.Lifecycle{
							PreStop: &corev1.LifecycleHandler{Exec: &corev1.ExecAction{Command: []string{"/bin/sh", "-c", DrainScript(wait)}}},
						},
						SecurityContext: &corev1.SecurityContext{
							AllowPrivilegeEscalation: ptr.To(false),
							Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
						},
					}},
					Volumes: []corev1.Volume{{
						Name: "serve-config",
						VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
							LocalObjectReference: corev1.LocalObjectReference{Name: s.Gateway},
						}},
					}},
				},
			},
		},
	}
}
