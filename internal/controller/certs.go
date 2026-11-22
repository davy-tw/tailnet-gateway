package controller

import (
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"

	"go.pkgs.davy.tw/tailnet-gateway/internal/config"
	"go.pkgs.davy.tw/tailnet-gateway/internal/proxy"
)

// Keys tailscaled writes in a certificate Secret in cert share mode.
const (
	certSecretKeyCert = "tls.crt"
	certSecretKeyKey  = "tls.key"
)

// Bookkeeping annotations on certificate Secrets that only the controller
// reads (see also proxy.AnnotationCertificate*).
const (
	// annotationCertificateObservedAt is when the controller first saw the
	// current certificate. A reading replica started before then may hold
	// the previous one.
	annotationCertificateObservedAt = config.Domain + "/certificate-observed-at"
	// annotationUnusedSince is when the hostname stopped being served. The
	// Secret is kept while reading replicas started before then exist:
	// they may hold its certificate in memory, and would serve it again
	// (even expired) if the hostname came back.
	annotationUnusedSince = config.Domain + "/unused-since"
)

// timeNow is the clock of the certificate bookkeeping, replaced in tests.
var timeNow = time.Now

// certRestartMargin is how long before the previous certificate expires
// the reading replicas still holding it are restarted. Tailscale renews
// when about a third of the lifetime is left (around 30 days for Let's
// Encrypt), so pods restarted for other reasons in between need nothing.
const certRestartMargin = 7 * 24 * time.Hour

// listReloadWindow is how long after a renewal a reading replica that may
// list Secrets can still hold the previous certificate: tailscaled reloads
// certificates daily.
const listReloadWindow = 25 * time.Hour

// certState is what the restart logic needs to know about one certificate
// Secret.
type certState struct {
	name string
	// observedAt is when the current certificate was first seen.
	observedAt time.Time
	// previousNotAfter is the expiry of the certificate before it, zero
	// if none was seen.
	previousNotAfter time.Time
}

// certSecrets returns the certificate Secrets the Gateway controls.
func (r *GatewayReconciler) certSecrets(ctx context.Context, gw *gwv1.Gateway) ([]corev1.Secret, error) {
	var l corev1.SecretList
	sel := client.MatchingLabels{
		proxy.LabelManagedBy: proxy.ManagedBy,
		proxy.LabelGateway:   gw.Name,
		proxy.LabelComponent: proxy.ComponentCertificate,
	}
	if err := r.List(ctx, &l, client.InNamespace(gw.Namespace), sel); err != nil {
		return nil, err
	}
	var out []corev1.Secret
	for _, s := range l.Items {
		if metav1.IsControlledBy(&s, gw) {
			out = append(out, s)
		}
	}
	return out, nil
}

// proxyPods returns the pods of the Gateway's proxy StatefulSet by
// ordinal.
func (r *GatewayReconciler) proxyPods(ctx context.Context, gw *gwv1.Gateway) (map[int]*corev1.Pod, error) {
	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(gw.Namespace), client.MatchingLabels(proxy.SelectorLabels(gw.Name))); err != nil {
		return nil, err
	}
	out := map[int]*corev1.Pod{}
	for i := range pods.Items {
		p := &pods.Items[i]
		if o := metav1.GetControllerOf(p); o == nil || o.Kind != "StatefulSet" || o.Name != gw.Name {
			continue
		}
		ord, err := strconv.Atoi(strings.TrimPrefix(p.Name, gw.Name+"-"))
		if err != nil {
			continue
		}
		out[ord] = p
	}
	return out, nil
}

// readerStartedBefore reports whether a replica other than 0 (the only one
// that writes certificates, and that reads them from the Secret every
// time) started before t.
func readerStartedBefore(pods map[int]*corev1.Pod, t time.Time) bool {
	for ord, p := range pods {
		if ord > 0 && p.CreationTimestamp.Time.Before(t) {
			return true
		}
	}
	return false
}

// syncCertSecrets keeps one certificate Secret per name in
// spec.CertDomains (none without certificate sharing), tracks the
// certificates written to them, and sets spec.CertGrants. It returns when
// to check again (zero for never).
//
// A Secret whose name is no longer served stays granted for
// certGrantGrace, as replica 0 may still serve the name until its mounted
// serve config catches up: without access, it would take the Secret for
// missing, complete an ACME order, fail to store the certificate and try
// again, using up Let's Encrypt's limit. It is then kept, without a grant,
// until no reading replica that started before may hold its certificate.
// With sharing turned off, the Secrets stay granted until no pod runs in
// cert share mode any more.
func (r *GatewayReconciler) syncCertSecrets(ctx context.Context, gw *gwv1.Gateway, spec *proxy.Spec, now time.Time) ([]certState, time.Duration, error) {
	existing, err := r.certSecrets(ctx, gw)
	if err != nil {
		return nil, 0, err
	}
	var pods map[int]*corev1.Pod
	if len(existing) > 0 {
		if pods, err = r.proxyPods(ctx, gw); err != nil {
			return nil, 0, err
		}
	}
	byName := map[string]*corev1.Secret{}
	for i := range existing {
		byName[existing[i].Name] = &existing[i]
	}
	want := map[string]bool{}
	for _, d := range spec.CertDomains {
		want[d] = true
	}
	names := append([]string(nil), spec.CertDomains...)
	for n := range byName {
		if !want[n] {
			names = append(names, n)
		}
	}
	sort.Strings(names)

	var next time.Duration
	later := func(d time.Duration) {
		if d > 0 && (next == 0 || d < next) {
			next = d
		}
	}
	sharing := spec.Params.CertificateSharing
	stillSharing := sharing || anyPodSharesCerts(pods)
	stamp := now.UTC().Format(time.RFC3339)
	spec.CertGrants = nil
	var states []certState
	for _, name := range names {
		cur := byName[name]
		if !stillSharing {
			if err := r.Delete(ctx, cur); client.IgnoreNotFound(err) != nil {
				return nil, 0, fmt.Errorf("deleting certificate Secret %s: %w", name, err)
			}
			continue
		}
		ann := map[string]string{}
		if cur != nil {
			for _, k := range []string{proxy.AnnotationCertificateSHA256, proxy.AnnotationCertificateNotAfter,
				proxy.AnnotationPreviousCertificateNotAfter, annotationCertificateObservedAt, annotationUnusedSince} {
				if v := cur.Annotations[k]; v != "" {
					ann[k] = v
				}
			}
			if sum, notAfter, ok := certInfo(cur); ok && sum != ann[proxy.AnnotationCertificateSHA256] {
				if ann[proxy.AnnotationCertificateSHA256] != "" {
					ann[proxy.AnnotationPreviousCertificateNotAfter] = ann[proxy.AnnotationCertificateNotAfter]
				}
				ann[proxy.AnnotationCertificateSHA256] = sum
				ann[proxy.AnnotationCertificateNotAfter] = notAfter.UTC().Format(time.RFC3339)
				ann[annotationCertificateObservedAt] = stamp
			}
		}
		grant := true
		switch {
		case want[name]:
			delete(ann, annotationUnusedSince)
		case !sharing:
			// Turned off: replica 0 may run in cert share mode until the
			// rollout reaches it, last.
		default:
			if ann[annotationUnusedSince] == "" {
				ann[annotationUnusedSince] = stamp
			}
			since := parseTime(ann[annotationUnusedSince])
			if left := since.Add(certGrantGrace).Sub(now); left > 0 {
				later(left)
				break
			}
			grant = false
			if !readerStartedBefore(pods, since) {
				if err := r.Delete(ctx, cur); client.IgnoreNotFound(err) != nil {
					return nil, 0, fmt.Errorf("deleting certificate Secret %s: %w", name, err)
				}
				continue
			}
		}
		if err := r.applyOwned(ctx, gw, spec.CertSecret(name, ann)); err != nil {
			return nil, 0, err
		}
		if grant {
			spec.CertGrants = append(spec.CertGrants, name)
		}
		if want[name] {
			states = append(states, certState{
				name:             name,
				observedAt:       parseTime(ann[annotationCertificateObservedAt]),
				previousNotAfter: parseTime(ann[proxy.AnnotationPreviousCertificateNotAfter]),
			})
		}
	}
	if !sharing && stillSharing {
		later(pendingRequeue)
	}
	return states, next, nil
}

// certGrantGrace is how long a name that is no longer served stays granted:
// longer than the kubelet takes to update a mounted ConfigMap (its sync
// period plus its cache TTL, a minute each by default).
const certGrantGrace = 5 * time.Minute

// anyPodSharesCerts reports whether a proxy pod runs in cert share mode.
func anyPodSharesCerts(pods map[int]*corev1.Pod) bool {
	for _, p := range pods {
		for _, c := range p.Spec.Containers {
			for _, e := range c.Env {
				if e.Name == proxy.EnvCertShare && e.Value == "true" {
					return true
				}
			}
		}
	}
	return false
}

// seedCertSecrets fills empty certificate Secrets with a valid certificate
// that a replica issued for itself before sharing was turned on, so that
// the replicas restarted in read-only mode have one at once (they never
// issue) and no new certificate is needed. Replica 0's is preferred.
func (r *GatewayReconciler) seedCertSecrets(ctx context.Context, gw *gwv1.Gateway, domains []string, now time.Time) error {
	if len(domains) == 0 {
		return nil
	}
	states, err := r.stateSecrets(ctx, gw)
	if err != nil {
		return err
	}
	sort.Slice(states, func(i, j int) bool { return states[i].Name < states[j].Name })
	if i := slices.IndexFunc(states, func(s corev1.Secret) bool { return s.Name == proxy.StateSecretName(gw.Name, 0) }); i > 0 {
		states[0], states[i] = states[i], states[0]
	}
	for _, d := range domains {
		// From the API server: the Secret may have been created just now,
		// and the state Secrets are stripped right after.
		sec := &corev1.Secret{}
		if err := r.reader().Get(ctx, client.ObjectKey{Namespace: gw.Namespace, Name: d}, sec); err != nil {
			return fmt.Errorf("reading certificate Secret %s: %w", d, err)
		}
		if len(sec.Data[certSecretKeyCert]) > 0 || !metav1.IsControlledBy(sec, gw) {
			continue
		}
		for _, st := range states {
			cert, key := st.Data[d+".crt"], st.Data[d+".key"]
			if len(key) == 0 || !certValidFor(cert, d, now) {
				continue
			}
			orig := sec.DeepCopy()
			sec.Data = map[string][]byte{certSecretKeyCert: cert, certSecretKeyKey: key}
			if err := r.Patch(ctx, sec, client.MergeFrom(orig)); err != nil {
				return fmt.Errorf("seeding certificate Secret %s: %w", d, err)
			}
			log.FromContext(ctx).Info("seeded shared certificate from a replica's own", "certificate", d, "from", st.Name)
			break
		}
	}
	return nil
}

// certValidFor reports whether certPEM is a certificate for name that is
// valid at now and for at least another day.
func certValidFor(certPEM []byte, name string, now time.Time) bool {
	block, _ := pem.Decode(certPEM)
	if block == nil {
		return false
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return false
	}
	return cert.VerifyHostname(name) == nil && now.After(cert.NotBefore) && now.Add(24*time.Hour).Before(cert.NotAfter)
}

// restartStaleReaders restarts, one at a time, the reading replicas that
// may still hold a certificate that was renewed, once that certificate is
// about to expire. It returns when to check again (zero for never).
//
// Replicas in tailscaled's read-only mode load a certificate from its
// Secret once and keep it in memory. Without list permission, tailscaled's
// daily reload does not reach them, so only a restart replaces it.
// Replica 0 reads the Secret every time and is never restarted.
func (r *GatewayReconciler) restartStaleReaders(ctx context.Context, gw *gwv1.Gateway, params config.ClassParams, states []certState, now time.Time) (time.Duration, error) {
	if !params.CertificateSharing || params.Replicas < 2 {
		return 0, nil
	}
	list := params.CertificateReload == config.CertificateReloadList
	var pods map[int]*corev1.Pod
	var next time.Duration
	due := map[int]string{}
	for _, st := range states {
		if st.previousNotAfter.IsZero() || st.observedAt.IsZero() {
			continue
		}
		if list && now.Sub(st.observedAt) > listReloadWindow {
			continue // reloaded by tailscaled
		}
		if pods == nil {
			var err error
			if pods, err = r.proxyPods(ctx, gw); err != nil {
				return 0, err
			}
		}
		if !readerStartedBefore(pods, st.observedAt) {
			continue
		}
		deadline := st.previousNotAfter.Add(-certRestartMargin)
		if now.Before(deadline) {
			if list {
				// The daily reload will have replaced it by then.
				continue
			}
			if d := deadline.Sub(now); next == 0 || d < next {
				next = d
			}
			continue
		}
		for ord, p := range pods {
			if ord > 0 && p.CreationTimestamp.Time.Before(st.observedAt) {
				due[ord] = st.name
			}
		}
	}
	if len(due) == 0 {
		return next, nil
	}
	// One replica at a time, and only while all the others serve.
	if int32(len(pods)) != params.Replicas {
		return pendingRequeue, nil
	}
	for _, p := range pods {
		if !p.DeletionTimestamp.IsZero() || !podReady(p) {
			return pendingRequeue, nil
		}
	}
	ords := make([]int, 0, len(due))
	for o := range due {
		ords = append(ords, o)
	}
	sort.Ints(ords)
	p := pods[ords[0]]
	log.FromContext(ctx).Info("restarting proxy replica to load a renewed certificate", "pod", p.Name, "certificate", due[ords[0]])
	r.event(gw, corev1.EventTypeNormal, "CertificateReload", "restarting %s: it may hold the previous certificate of %s, which expires soon", p.Name, due[ords[0]])
	if err := r.Delete(ctx, p); client.IgnoreNotFound(err) != nil {
		return 0, err
	}
	return pendingRequeue, nil
}

func podReady(p *corev1.Pod) bool {
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

func parseTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}
	}
	return t
}

// certInfo returns the SHA-256 and expiry of the certificate in a
// certificate Secret; ok is false while it has none, or none that parses.
func certInfo(s *corev1.Secret) (sum string, notAfter time.Time, ok bool) {
	c := s.Data[certSecretKeyCert]
	if len(c) == 0 || len(s.Data[certSecretKeyKey]) == 0 {
		return "", time.Time{}, false
	}
	block, _ := pem.Decode(c)
	if block == nil {
		return "", time.Time{}, false
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return "", time.Time{}, false
	}
	h := sha256.Sum256(c)
	return hex.EncodeToString(h[:]), cert.NotAfter, true
}

// stripStateCerts removes the certificates that replicas issued for
// themselves from their state Secrets. tailscaled loads the whole state
// Secret into memory and looks there before the shared Secret, so a
// leftover certificate would be served, and once expired never replaced.
func (r *GatewayReconciler) stripStateCerts(ctx context.Context, gw *gwv1.Gateway) error {
	secrets, err := r.stateSecrets(ctx, gw)
	if err != nil {
		return err
	}
	for i := range secrets {
		s := &secrets[i]
		orig := s.DeepCopy()
		changed := false
		for k := range s.Data {
			if strings.HasSuffix(k, ".ts.net.crt") || strings.HasSuffix(k, ".ts.net.key") {
				delete(s.Data, k)
				changed = true
			}
		}
		if !changed {
			continue
		}
		if err := r.Patch(ctx, s, client.MergeFrom(orig)); client.IgnoreNotFound(err) != nil {
			return fmt.Errorf("removing per-replica certificates from state Secret %s: %w", s.Name, err)
		}
	}
	return nil
}
