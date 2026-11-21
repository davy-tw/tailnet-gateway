// Package proxy builds the Kubernetes objects that make up a Gateway's
// data plane: a StatefulSet of Tailscale containers serving the generated
// serve config, with per-replica state Secrets.
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

	AppName        = "tailnet-gateway-proxy"
	ManagedBy      = "tailnet-gateway"
	ComponentState = "state"

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

// Role lets the proxy pods read and update only their own state Secrets.
func (s *Spec) Role() *rbacv1.Role {
	names := make([]string, 0, s.Params.Replicas)
	for i := 0; i < int(s.Params.Replicas); i++ {
		names = append(names, StateSecretName(s.Gateway, i))
	}
	return &rbacv1.Role{
		ObjectMeta: s.meta(s.Gateway),
		Rules: []rbacv1.PolicyRule{
			{APIGroups: []string{""}, Resources: []string{"secrets"}, ResourceNames: names, Verbs: []string{"get", "update", "patch"}},
			{APIGroups: []string{""}, Resources: []string{"events"}, Verbs: []string{"create", "patch"}},
		},
	}
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

// servicesInConfig lists the "svc:" names in the mounted serve config.
const servicesInConfig = `grep -o '"svc:[a-z0-9-]*"' "$CFG" 2>/dev/null | tr -d '"' | sort -u`

// advertisedServices lists the "svc:" names the replica advertises, from
// the AdvertiseServices field of its prefs (printed as indented JSON).
const advertisedServices = `$TS debug prefs 2>/dev/null | tr -d ' \t\n' | grep -o '"AdvertiseServices":\[[^]]*\]' | grep -o '"svc:[a-z0-9-]*"' | tr -d '"' | sort -u`

// DrainingMarker is created by the preStop hook, so that the readiness
// probe stops advertising Services while the replica drains.
const DrainingMarker = "/tmp/tailnet-gateway-draining"

// ReadyScript reports a replica ready once tailscaled is up, runs the
// mounted serve config and advertises exactly the Tailscale Services in it.
//
// The serve configs are compared by their strings (keys, hostnames, paths
// and targets), which catches any change of backend, path, port or mode;
// the mounted file is written with the same field names. An IP alone does
// not mean the replica serves the current config.
//
// Advertisement is reconciled here too, once the config is applied: names
// added to the config are advertised and names removed from it are
// drained. The image only advertises the Services of the config it starts
// with, so without this a Gateway change that adds a Service would never
// be advertised. A draining replica (see DrainingMarker) is not ready and
// advertises nothing.
func ReadyScript() string {
	return strings.Join([]string{
		"CFG=" + shellQuote(ServeConfigDir+"/"+ServeConfigKey),
		"TS=" + shellQuote("tailscale --socket="+SocketPath),
		`[ -e ` + shellQuote(DrainingMarker) + ` ] && exit 1`,
		`$TS status >/dev/null 2>&1 || exit 1`,
		`want=$(grep -o '"[^"]*"' "$CFG" | sort -u)`,
		`have=$($TS serve status --json 2>/dev/null | grep -o '"[^"]*"' | sort -u)`,
		`[ "$want" = "$have" ] || exit 1`,
		`want=$(` + servicesInConfig + `)`,
		`have=$(` + advertisedServices + `)`,
		`[ "$want" = "$have" ] && exit 0`,
		`for s in $want; do printf '%s\n' "$have" | grep -qxF "$s" || $TS serve advertise "$s"; done`,
		`for s in $have; do printf '%s\n' "$want" | grep -qxF "$s" || $TS serve drain "$s"; done`,
		`[ "$want" = "$(` + advertisedServices + `)" ]`,
	}, "\n")
}

// DrainScript stops advertising every Tailscale Service, then waits so that
// open connections can finish before the container is stopped.
func DrainScript(waitSeconds int64) string {
	return strings.Join([]string{
		"CFG=" + shellQuote(ServeConfigDir+"/"+ServeConfigKey),
		"TS=" + shellQuote("tailscale --socket="+SocketPath),
		`touch ` + shellQuote(DrainingMarker),
		`for s in $(` + servicesInConfig + `; ` + advertisedServices + `); do $TS serve drain "$s" || true; done`,
		fmt.Sprintf("sleep %d", waitSeconds),
		`exit 0`,
	}, "\n")
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
							TimeoutSeconds:   5,
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
