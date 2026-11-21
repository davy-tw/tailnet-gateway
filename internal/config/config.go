// Package config holds controller-wide settings and per-GatewayClass
// parameters.
package config

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/validation"
)

// Domain prefixes every annotation, label, finalizer and controller name
// owned by this project.
const Domain = "tailnet-gateway.davy.tw"

// Well-known names derived from Domain.
const (
	DefaultControllerName  = Domain + "/gateway-controller"
	IngressControllerName  = Domain + "/ingress-controller"
	Finalizer              = Domain + "/finalizer"
	AnnotationGatewayClass = Domain + "/gateway-class"
	AnnotationHostname     = Domain + "/hostname"
	AnnotationSource       = Domain + "/source"
	// AnnotationTailscaleService on a TCPRoute or TLSRoute picks the
	// Tailscale Service name (without the "svc:" prefix) instead of the
	// route name. Generated routes use it so that every port of a
	// LoadBalancer Service shares one Tailscale Service.
	AnnotationTailscaleService = Domain + "/tailscale-service"

	// AnnotationTailscaleServices on a Gateway's serve config ConfigMap
	// lists every Tailscale Service name the Gateway holds, including ones
	// not in the config yet (see the Gateway reconciler's claims).
	AnnotationTailscaleServices = Domain + "/tailscale-services"

	// AnnotationDevices on a Gateway lists the tailnet device IDs of its
	// replicas, so that they can be deleted with the Gateway even after
	// garbage collection has removed the state Secrets (foreground
	// deletion).
	AnnotationDevices = Domain + "/devices"

	// AnnotationListenerSets on a Gateway lists the ListenerSets
	// ("namespace/name") whose status it wrote, so that one that moves to
	// a Gateway no Gateway of this controller serves can be released.
	AnnotationListenerSets = Domain + "/listener-sets"

	LabelSourceKind = Domain + "/source-kind"
	LabelSourceName = Domain + "/source-name"

	SourceIngress = "ingress"
	SourceService = "service"
)

// DefaultProxyImage is the pinned Tailscale image used for proxy pods.
// Tailscale Services hosting needs v1.86.0 or later. When bumping it,
// recheck that "tailscale debug prefs" still prints an AdvertiseServices
// field (see proxy.ReadyScript): without it, no replica becomes ready.
const DefaultProxyImage = "tailscale/tailscale:v1.104.7"

// MaxReplicas bounds the proxy replicas per Gateway. Each replica is a
// tailnet device with its own state Secret, so the bound also keeps a typo
// from minting thousands of keys.
const MaxReplicas = 64

// ClassParams are the settings that a GatewayClass may override.
type ClassParams struct {
	Tags             []string
	Replicas         int32
	Image            string
	DrainGracePeriod time.Duration
}

// Config is the controller-wide configuration.
type Config struct {
	OAuthClientID     string
	OAuthClientSecret string
	TailscaleAPIURL   string

	ControllerName      string
	DefaultGatewayClass string
	IngressClass        string
	LoadBalancerClass   string

	Defaults ClassParams

	AuthKeyDescription string
	MetricsAddr        string
	ProbeAddr          string
	ClusterDomain      string
	// TailnetDomain, when set, is the tailnet's MagicDNS suffix (for
	// example "tail1234.ts.net"). Otherwise it is learned from the first
	// proxy replica that logs in.
	TailnetDomain string
	// Namespace the controller runs in (for leader election).
	Namespace string
}

// OAuthEnabled reports whether the Tailscale API can be used.
func (c *Config) OAuthEnabled() bool {
	return c.OAuthClientID != "" && c.OAuthClientSecret != ""
}

// Environment variable names.
const (
	EnvOAuthClientID       = "TAILSCALE_OAUTH_CLIENT_ID"
	EnvOAuthClientSecret   = "TAILSCALE_OAUTH_CLIENT_SECRET"
	EnvTailscaleAPIURL     = "TAILSCALE_API_URL"
	EnvTags                = "PROXY_TAGS"
	EnvControllerName      = "GATEWAY_CONTROLLER_NAME"
	EnvDefaultGatewayClass = "DEFAULT_GATEWAY_CLASS"
	EnvIngressClass        = "INGRESS_CLASS"
	EnvLoadBalancerClass   = "LOAD_BALANCER_CLASS"
	EnvReplicas            = "PROXY_REPLICAS"
	EnvDrainGracePeriod    = "PROXY_DRAIN_GRACE_PERIOD"
	EnvImage               = "PROXY_IMAGE"
	EnvAuthKeyDescription  = "AUTH_KEY_DESCRIPTION"
	EnvMetricsAddr         = "METRICS_BIND_ADDRESS"
	EnvProbeAddr           = "HEALTH_PROBE_BIND_ADDRESS"
	EnvClusterDomain       = "CLUSTER_DOMAIN"
	EnvTailnetDomain       = "TAILNET_DOMAIN"
	EnvNamespace           = "POD_NAMESPACE"
)

// FromEnv reads the configuration from the process environment.
func FromEnv() (*Config, error) { return Load(os.Getenv) }

// Load reads the configuration through getenv.
func Load(getenv func(string) string) (*Config, error) {
	get := func(k, def string) string {
		if v := strings.TrimSpace(getenv(k)); v != "" {
			return v
		}
		return def
	}
	c := &Config{
		OAuthClientID:       get(EnvOAuthClientID, ""),
		OAuthClientSecret:   get(EnvOAuthClientSecret, ""),
		TailscaleAPIURL:     get(EnvTailscaleAPIURL, "https://api.tailscale.com"),
		ControllerName:      get(EnvControllerName, DefaultControllerName),
		DefaultGatewayClass: get(EnvDefaultGatewayClass, "tailnet"),
		IngressClass:        get(EnvIngressClass, "tailnet"),
		LoadBalancerClass:   get(EnvLoadBalancerClass, "tailnet"),
		AuthKeyDescription:  get(EnvAuthKeyDescription, "tailnet-gateway proxy"),
		MetricsAddr:         get(EnvMetricsAddr, ":8080"),
		ProbeAddr:           get(EnvProbeAddr, ":8081"),
		ClusterDomain:       get(EnvClusterDomain, "cluster.local"),
		TailnetDomain:       strings.Trim(get(EnvTailnetDomain, ""), "."),
		Namespace:           get(EnvNamespace, ""),
	}
	base := ClassParams{
		Replicas:         2,
		Image:            DefaultProxyImage,
		DrainGracePeriod: 30 * time.Second,
	}
	defaults, _, err := ParseClassParams(map[string]string{
		ParamTags:             getenv(EnvTags),
		ParamReplicas:         getenv(EnvReplicas),
		ParamImage:            getenv(EnvImage),
		ParamDrainGracePeriod: getenv(EnvDrainGracePeriod),
	}, base)
	if err != nil {
		return nil, fmt.Errorf("controller defaults: %w", err)
	}
	c.Defaults = defaults
	if errs := validation.IsQualifiedName(c.LoadBalancerClass); len(errs) > 0 {
		return nil, fmt.Errorf("%s %q: %s", EnvLoadBalancerClass, c.LoadBalancerClass, strings.Join(errs, "; "))
	}
	return c, nil
}

// Keys recognized in a GatewayClass parameters ConfigMap.
const (
	ParamTags             = "tags"
	ParamReplicas         = "replicas"
	ParamImage            = "image"
	ParamDrainGracePeriod = "drainGracePeriod"
)

// ParseClassParams overlays data on defaults. Empty values fall back to
// the default. It returns the unknown keys (sorted) so that the caller can
// warn about them.
func ParseClassParams(data map[string]string, defaults ClassParams) (ClassParams, []string, error) {
	p := defaults
	p.Tags = append([]string(nil), defaults.Tags...)
	var unknown []string
	for k, raw := range data {
		v := strings.TrimSpace(raw)
		switch k {
		case ParamTags:
			if v == "" {
				continue
			}
			tags, err := ParseTags(v)
			if err != nil {
				return p, nil, err
			}
			p.Tags = tags
		case ParamReplicas:
			if v == "" {
				continue
			}
			n, err := strconv.ParseInt(v, 10, 32)
			if err != nil || n < 1 || n > MaxReplicas {
				return p, nil, fmt.Errorf("%s: %q is not an integer from 1 to %d", ParamReplicas, v, MaxReplicas)
			}
			p.Replicas = int32(n)
		case ParamImage:
			if v == "" {
				continue
			}
			if strings.ContainsAny(v, " \t\n") {
				return p, nil, fmt.Errorf("%s: %q is not a valid image reference", ParamImage, v)
			}
			p.Image = v
		case ParamDrainGracePeriod:
			if v == "" {
				continue
			}
			d, err := time.ParseDuration(v)
			if err != nil || d < 0 {
				return p, nil, fmt.Errorf("%s: %q is not a valid non-negative duration", ParamDrainGracePeriod, v)
			}
			p.DrainGracePeriod = d
		default:
			unknown = append(unknown, k)
		}
	}
	sort.Strings(unknown)
	return p, unknown, nil
}

// ParseTags splits a comma-separated tag list, trimming blanks. Every tag
// must look like "tag:<name>".
func ParseTags(s string) ([]string, error) {
	var out []string
	for _, t := range strings.Split(s, ",") {
		t = strings.TrimSpace(t)
		if t == "" {
			continue
		}
		name, ok := strings.CutPrefix(t, "tag:")
		if !ok || name == "" || strings.ContainsAny(name, " :") {
			return nil, fmt.Errorf("%s: %q is not a valid tag (expected tag:<name>)", ParamTags, t)
		}
		out = append(out, t)
	}
	return out, nil
}
