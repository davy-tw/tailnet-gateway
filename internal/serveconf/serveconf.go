// Package serveconf models the JSON serve config file that the Tailscale
// container image applies (the same document "tailscale serve status
// --json" prints). Only the parts used for Tailscale Services are modeled.
package serveconf

import (
	"encoding/json"
	"sort"
	"strconv"
	"strings"
)

// Config is a serve config. Only the Services section is used: every
// listener belongs to a Tailscale Service.
type Config struct {
	Services map[string]*Service `json:"Services,omitempty"`
}

// Service is the serve configuration for one Tailscale Service
// ("svc:<name>").
type Service struct {
	// TCP maps a service port to how connections on it are handled.
	TCP map[uint16]*TCPPortHandler `json:"TCP,omitempty"`
	// Web maps "<service fqdn>:<port>" to HTTP handlers.
	Web map[string]*WebServer `json:"Web,omitempty"`
}

// TCPPortHandler describes one port of a Service.
type TCPPortHandler struct {
	HTTPS bool `json:"HTTPS,omitempty"`
	HTTP  bool `json:"HTTP,omitempty"`
	// TCPForward is a host:port to forward raw connections to.
	TCPForward string `json:"TCPForward,omitempty"`
	// TerminateTLS, with TCPForward, terminates TLS for this server name
	// before forwarding.
	TerminateTLS string `json:"TerminateTLS,omitempty"`
}

// WebServer holds the handlers for one host:port.
type WebServer struct {
	Handlers map[string]*HTTPHandler `json:"Handlers"`
}

// HTTPHandler is one mount point.
type HTTPHandler struct {
	Proxy string `json:"Proxy,omitempty"`
}

// ServiceName returns the "svc:" form of a service label.
func ServiceName(label string) string { return "svc:" + label }

// WebKey returns the Web map key for a service on a port.
func WebKey(label, tailnetDomain string, port uint16) string {
	return label + "." + tailnetDomain + ":" + strconv.Itoa(int(port))
}

// Marshal renders the config deterministically (map keys are sorted by
// encoding/json), indented for readability.
func (c *Config) Marshal() ([]byte, error) {
	if c == nil {
		c = &Config{}
	}
	return json.MarshalIndent(c, "", "  ")
}

// ServiceNames returns the sorted "svc:" names in the config.
func (c *Config) ServiceNames() []string {
	if c == nil {
		return nil
	}
	out := make([]string, 0, len(c.Services))
	for n := range c.Services {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// Labels returns the sorted service names without the "svc:" prefix.
func (c *Config) Labels() []string {
	names := c.ServiceNames()
	for i, n := range names {
		names[i] = strings.TrimPrefix(n, "svc:")
	}
	return names
}
