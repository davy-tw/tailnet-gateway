package serveconf

import (
	"reflect"
	"testing"
)

func TestCertDomains(t *testing.T) {
	c := &Config{Services: map[string]*Service{
		"svc:web": {
			TCP: map[uint16]*TCPPortHandler{443: {HTTPS: true}, 80: {HTTP: true}},
			Web: map[string]*WebServer{"web.tail1234.ts.net:443": {}, "web.tail1234.ts.net:80": {}},
		},
		"svc:alt": {
			TCP: map[uint16]*TCPPortHandler{8443: {HTTPS: true}},
			Web: map[string]*WebServer{"alt.tail1234.ts.net:8443": {}},
		},
		"svc:db": {TCP: map[uint16]*TCPPortHandler{
			5432: {TCPForward: "db:5432", TerminateTLS: "db.tail1234.ts.net"},
			22:   {TCPForward: "ssh:22"},
		}},
		// Tailnet domain not known yet.
		"svc:early": {
			TCP: map[uint16]*TCPPortHandler{443: {HTTPS: true}},
			Web: map[string]*WebServer{"early.:443": {}},
		},
	}}
	want := []string{"alt.tail1234.ts.net", "db.tail1234.ts.net", "web.tail1234.ts.net"}
	if got := c.CertDomains(); !reflect.DeepEqual(got, want) {
		t.Errorf("CertDomains = %v, want %v", got, want)
	}
	if got := (*Config)(nil).CertDomains(); got != nil {
		t.Errorf("nil config: %v", got)
	}
}
