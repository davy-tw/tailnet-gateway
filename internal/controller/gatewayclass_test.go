package controller

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"

	"go.pkgs.davy.tw/tailnet-gateway/internal/config"
)

// With an OAuth client, a class must end up with at least one tag, since
// the Tailscale API mints no untagged keys for OAuth clients. Manual key
// mode has no such requirement.
func TestClassParamsRequireTagsWithOAuth(t *testing.T) {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: "infra", Name: "params"},
		Data:       map[string]string{config.ParamTags: "tag:prod"},
	}
	blank := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: "infra", Name: "blank"},
		Data:       map[string]string{config.ParamTags: "", config.ParamReplicas: "1"},
	}
	class := func(params string) *gwv1.GatewayClass {
		gc := &gwv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: "tailnet"}}
		if params != "" {
			gc.Spec.ParametersRef = &gwv1.ParametersReference{Kind: "ConfigMap", Name: params, Namespace: ptr.To(gwv1.Namespace("infra"))}
		}
		return gc
	}
	cases := []struct {
		name    string
		env     map[string]string
		params  string
		invalid bool
		tags    string
	}{
		{name: "oauth, no tags anywhere", env: map[string]string{config.EnvTags: ""}, invalid: true},
		{name: "oauth, blank class tags", params: "blank", invalid: true},
		{name: "oauth, controller tags", env: map[string]string{config.EnvTags: "tag:k8s"}, tags: "tag:k8s"},
		{name: "oauth, class tags", params: "params", tags: "tag:prod"},
		{name: "oauth, blank class tags keep the default", env: map[string]string{config.EnvTags: "tag:k8s"}, params: "blank", tags: "tag:k8s"},
		{name: "manual keys, no tags", env: map[string]string{config.EnvOAuthClientID: "-"}, tags: ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := map[string]string{config.EnvOAuthClientID: "id", config.EnvOAuthClientSecret: "secret"}
			for k, v := range tc.env {
				env[k] = v
			}
			if env[config.EnvOAuthClientID] == "-" {
				delete(env, config.EnvOAuthClientID)
			}
			cfg, err := config.Load(func(k string) string { return env[k] })
			if err != nil {
				t.Fatal(err)
			}
			c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(cm, blank).Build()
			r := &classResolver{Client: c, Config: cfg}
			p, _, err := r.classParams(context.Background(), class(tc.params))
			var ip *invalidParamsError
			if tc.invalid {
				if !asInvalid(err, &ip) || !strings.Contains(ip.msg, "no tags") {
					t.Fatalf("got %v, want an invalid-parameters error about tags", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got := strings.Join(p.Tags, ","); got != tc.tags {
				t.Errorf("tags = %q, want %q", got, tc.tags)
			}
		})
	}
}
