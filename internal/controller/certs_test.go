package controller

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"go.pkgs.davy.tw/tailnet-gateway/internal/proxy"
)

// TestTrackCertificate covers when the reading replicas must be restarted
// after a renewal.
func TestTrackCertificate(t *testing.T) {
	day := 24 * time.Hour
	now := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	secret := func(notBefore, notAfter time.Time) *corev1.Secret {
		return &corev1.Secret{Data: map[string][]byte{
			"tls.crt": testCert(t, "gw.tail1234.ts.net", notBefore, notAfter), "tls.key": []byte("k")}}
	}
	// A reader started long ago, and replica 0, which never counts.
	pods := map[int]*corev1.Pod{
		0: {ObjectMeta: metav1.ObjectMeta{CreationTimestamp: metav1.NewTime(now.Add(-200 * day))}},
		1: {ObjectMeta: metav1.ObjectMeta{CreationTimestamp: metav1.NewTime(now.Add(-200 * day))}},
	}
	by := func(ann map[string]string) time.Time { return parseTime(ann[proxy.AnnotationRestartReadersBy]) }

	// First certificate (issued 60 days ago, 30 left): nothing to restart.
	ann := map[string]string{}
	c1 := secret(now.Add(-60*day), now.Add(30*day))
	trackCertificate(ann, c1, pods, now)
	if !by(ann).IsZero() {
		t.Errorf("restart-by after the first certificate = %v", by(ann))
	}

	// Usual renewal: 7 days before the replaced one expires.
	trackCertificate(ann, secret(now, now.Add(90*day)), pods, now)
	if want := now.Add(23 * day); !by(ann).Equal(want) {
		t.Errorf("restart-by = %v, want %v", by(ann), want)
	}

	// Renewed again before the readers restarted (the new one was revoked,
	// say): early, so at once, and never later than before.
	later := now.Add(2 * day)
	c3 := secret(later, later.Add(90*day))
	trackCertificate(ann, c3, pods, later)
	if !by(ann).Equal(later) {
		t.Errorf("restart-by after an early renewal = %v, want %v", by(ann), later)
	}

	// Once every reader started after the current certificate, it goes.
	pods[1] = &corev1.Pod{ObjectMeta: metav1.ObjectMeta{CreationTimestamp: metav1.NewTime(later.Add(time.Hour))}}
	trackCertificate(ann, c3, pods, later.Add(2*time.Hour))
	if !by(ann).IsZero() {
		t.Errorf("restart-by kept after the readers restarted: %v", by(ann))
	}

	// Two usual renewals before the readers restart keep the earlier
	// deadline: the oldest certificate a reader may hold expires first.
	pods[1] = &corev1.Pod{ObjectMeta: metav1.ObjectMeta{CreationTimestamp: metav1.NewTime(now.Add(-200 * day))}}
	ann = map[string]string{}
	trackCertificate(ann, secret(now.Add(-60*day), now.Add(30*day)), pods, now)
	trackCertificate(ann, secret(now.Add(-35*day), now.Add(55*day)), pods, now) // usual: 30 of 90 days left
	first := by(ann)
	trackCertificate(ann, secret(now.Add(-10*day), now.Add(80*day)), pods, now) // 55 of 90 left: early
	if !by(ann).Equal(now) || !first.Equal(now.Add(23*day)) {
		t.Errorf("restart-by = %v (first %v)", by(ann), first)
	}
	_ = c1
}
