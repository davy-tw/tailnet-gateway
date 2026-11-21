package config

import (
	"reflect"
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	c, err := Load(func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	if c.OAuthEnabled() {
		t.Error("OAuth should be disabled without credentials")
	}
	if c.DefaultGatewayClass != "tailnet" || c.IngressClass != "tailnet" || c.LoadBalancerClass != "tailnet" {
		t.Errorf("unexpected class defaults: %+v", c)
	}
	if c.ControllerName != DefaultControllerName {
		t.Errorf("controller name = %q", c.ControllerName)
	}
	want := ClassParams{Replicas: 2, Image: DefaultProxyImage, DrainGracePeriod: 30 * time.Second}
	if !reflect.DeepEqual(c.Defaults, want) {
		t.Errorf("defaults = %+v, want %+v", c.Defaults, want)
	}
}

func TestLoadOverrides(t *testing.T) {
	env := map[string]string{
		EnvOAuthClientID:     "id",
		EnvOAuthClientSecret: "secret",
		EnvTags:              " tag:a , ,tag:b ",
		EnvReplicas:          "3",
		EnvLoadBalancerClass: "example.com/tailnet",
		EnvTailnetDomain:     "tail1234.ts.net.",
	}
	c, err := Load(func(k string) string { return env[k] })
	if err != nil {
		t.Fatal(err)
	}
	if !c.OAuthEnabled() {
		t.Error("OAuth should be enabled")
	}
	if !reflect.DeepEqual(c.Defaults.Tags, []string{"tag:a", "tag:b"}) {
		t.Errorf("tags = %v", c.Defaults.Tags)
	}
	if c.Defaults.Replicas != 3 {
		t.Errorf("replicas = %d", c.Defaults.Replicas)
	}
	if c.TailnetDomain != "tail1234.ts.net" {
		t.Errorf("tailnet domain = %q", c.TailnetDomain)
	}
}

func TestLoadInvalid(t *testing.T) {
	for name, env := range map[string]map[string]string{
		"replicas": {EnvReplicas: "two"},
		"lbclass":  {EnvLoadBalancerClass: "not a class"},
		"tags":     {EnvTags: "prod"},
	} {
		if _, err := Load(func(k string) string { return env[k] }); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}

func TestParseClassParams(t *testing.T) {
	def := ClassParams{Tags: []string{"tag:default"}, Replicas: 2, Image: "img", DrainGracePeriod: time.Second}
	p, unknown, err := ParseClassParams(map[string]string{
		"tags":             "tag:prod",
		"replicas":         "3",
		"drainGracePeriod": "45s",
		"color":            "blue",
	}, def)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(p.Tags, []string{"tag:prod"}) || p.Replicas != 3 || p.Image != "img" || p.DrainGracePeriod != 45*time.Second {
		t.Errorf("params = %+v", p)
	}
	if !reflect.DeepEqual(unknown, []string{"color"}) {
		t.Errorf("unknown = %v", unknown)
	}
	for _, bad := range []map[string]string{
		{"replicas": "0"}, {"replicas": "x"}, {"drainGracePeriod": "soon"}, {"tags": "tag:"}, {"image": "a b"},
	} {
		if _, _, err := ParseClassParams(bad, def); err == nil {
			t.Errorf("%v: expected error", bad)
		}
	}
}
