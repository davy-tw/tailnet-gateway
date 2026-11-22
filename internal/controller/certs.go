package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"

	"go.pkgs.davy.tw/tailnet-gateway/internal/proxy"
)

// Keys tailscaled writes in a certificate Secret in cert share mode.
const (
	certSecretKeyCert = "tls.crt"
	certSecretKeyKey  = "tls.key"
)

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

// syncCertSecrets keeps one certificate Secret per name in
// spec.CertDomains (none without certificate sharing), deletes the
// others, and sets spec.CertHash from the renewed certificates.
//
// The replicas that only read certificates fetch one from its Secret the
// first time they need it and then keep it in memory: they cannot list
// Secrets, so Tailscale's daily reload does not reach them. A renewal
// therefore changes spec.CertHash, which rolls the replicas. The first
// certificate written to a Secret does not, since no replica holds one
// yet.
func (r *GatewayReconciler) syncCertSecrets(ctx context.Context, gw *gwv1.Gateway, spec *proxy.Spec) error {
	existing, err := r.certSecrets(ctx, gw)
	if err != nil {
		return err
	}
	byName := map[string]*corev1.Secret{}
	for i := range existing {
		byName[existing[i].Name] = &existing[i]
	}
	want := map[string]bool{}
	var renewed []string
	for _, d := range spec.CertDomains {
		want[d] = true
		var issued string
		if cur := byName[d]; cur != nil {
			issued = cur.Annotations[proxy.AnnotationIssuedCertificate]
			if sum := certSum(cur); sum != "" {
				if issued == "" {
					issued = sum
				} else if sum != issued {
					renewed = append(renewed, d+"="+sum)
				}
			}
		}
		if err := r.applyOwned(ctx, gw, spec.CertSecret(d, issued)); err != nil {
			return err
		}
	}
	for name, s := range byName {
		if want[name] {
			continue
		}
		if err := r.Delete(ctx, s); client.IgnoreNotFound(err) != nil {
			return fmt.Errorf("deleting certificate Secret %s: %w", name, err)
		}
	}
	spec.CertHash = ""
	if len(renewed) > 0 {
		sort.Strings(renewed)
		h := sha256.Sum256([]byte(strings.Join(renewed, "\n")))
		spec.CertHash = hex.EncodeToString(h[:8])
	}
	return nil
}

// certSum is the SHA-256 of the certificate in a certificate Secret, or ""
// while it has none.
func certSum(s *corev1.Secret) string {
	c := s.Data[certSecretKeyCert]
	if len(c) == 0 || len(s.Data[certSecretKeyKey]) == 0 {
		return ""
	}
	h := sha256.Sum256(c)
	return hex.EncodeToString(h[:])
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
