package translate

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/davy-tw/tailnet-gateway/internal/config"
	"github.com/davy-tw/tailnet-gateway/internal/serveconf"
)

const domain = "tail1234.ts.net"

func gateway(listeners ...gwv1.Listener) *gwv1.Gateway {
	return &gwv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Name: "gw", Namespace: "infra", Generation: 3},
		Spec:       gwv1.GatewaySpec{GatewayClassName: "tailnet", Listeners: listeners},
	}
}

func listener(name string, proto gwv1.ProtocolType, port int32) gwv1.Listener {
	return gwv1.Listener{Name: gwv1.SectionName(name), Protocol: proto, Port: gwv1.PortNumber(port)}
}

func allNamespaces(l gwv1.Listener) gwv1.Listener {
	from := gwv1.NamespacesFromAll
	l.AllowedRoutes = &gwv1.AllowedRoutes{Namespaces: &gwv1.RouteNamespaces{From: &from}}
	return l
}

func parent(name string) gwv1.ParentReference {
	ns := gwv1.Namespace("infra")
	return gwv1.ParentReference{Name: gwv1.ObjectName(name), Namespace: &ns}
}

func backendRef(name string, port int32) gwv1.BackendRef {
	return gwv1.BackendRef{BackendObjectReference: gwv1.BackendObjectReference{
		Name: gwv1.ObjectName(name), Port: ptr.To(gwv1.PortNumber(port)),
	}}
}

func prefix(p string) gwv1.HTTPRouteMatch {
	return gwv1.HTTPRouteMatch{Path: &gwv1.HTTPPathMatch{Type: ptr.To(gwv1.PathMatchPathPrefix), Value: ptr.To(p)}}
}

func httpRoute(ns, name string, created int, hostnames []string, rules ...gwv1.HTTPRouteRule) *gwv1.HTTPRoute {
	r := &gwv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Generation: 1,
			CreationTimestamp: metav1.NewTime(time.Unix(int64(created), 0))},
		Spec: gwv1.HTTPRouteSpec{CommonRouteSpec: gwv1.CommonRouteSpec{ParentRefs: []gwv1.ParentReference{parent("gw")}}, Rules: rules},
	}
	for _, h := range hostnames {
		r.Spec.Hostnames = append(r.Spec.Hostnames, gwv1.Hostname(h))
	}
	return r
}

func rule(be gwv1.BackendRef, matches ...gwv1.HTTPRouteMatch) gwv1.HTTPRouteRule {
	return gwv1.HTTPRouteRule{Matches: matches, BackendRefs: []gwv1.HTTPBackendRef{{BackendRef: be}}}
}

func mustJSON(t *testing.T, c *serveconf.Config) string {
	t.Helper()
	b, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func condOf(conds []metav1.Condition, typ string) *metav1.Condition {
	for i := range conds {
		if conds[i].Type == typ {
			return &conds[i]
		}
	}
	return nil
}

func routeConds(t *testing.T, res *Result, kind, name string) []metav1.Condition {
	t.Helper()
	for _, r := range res.Routes {
		if r.Ref.Kind == kind && r.Ref.Name == name {
			if len(r.Parents) != 1 {
				t.Fatalf("%s %s: %d parents", kind, name, len(r.Parents))
			}
			return r.Parents[0].Conditions
		}
	}
	t.Fatalf("no result for %s %s", kind, name)
	return nil
}

func expectCond(t *testing.T, conds []metav1.Condition, typ string, status metav1.ConditionStatus, reason string) {
	t.Helper()
	c := condOf(conds, typ)
	if c == nil {
		t.Fatalf("condition %s missing in %+v", typ, conds)
	}
	if c.Status != status || (reason != "" && c.Reason != reason) {
		t.Fatalf("condition %s = %s/%s (%s), want %s/%s", typ, c.Status, c.Reason, c.Message, status, reason)
	}
}

func TestDemoSplit(t *testing.T) {
	in := &Input{
		Gateway:       gateway(listener("http", gwv1.HTTPProtocolType, 80), listener("https", gwv1.HTTPSProtocolType, 443)),
		ClusterDomain: "cluster.local",
		TailnetDomain: domain,
		HTTPRoutes: []*gwv1.HTTPRoute{httpRoute("infra", "demo", 1, []string{"demo"},
			rule(backendRef("api", 8080), prefix("/api/")),
			rule(backendRef("web", 80)),
		)},
	}
	res := Translate(in)
	want := `{"Services":{"svc:demo":{"TCP":{"443":{"HTTPS":true},"80":{"HTTP":true}},"Web":{` +
		`"demo.tail1234.ts.net:443":{"Handlers":{"/":{"Proxy":"http://web.infra.svc.cluster.local:80/"},"/api":{"Proxy":"http://api.infra.svc.cluster.local:8080/api"}}},` +
		`"demo.tail1234.ts.net:80":{"Handlers":{"/":{"Proxy":"http://web.infra.svc.cluster.local:80/"},"/api":{"Proxy":"http://api.infra.svc.cluster.local:8080/api"}}}}}}}`
	if got := mustJSON(t, res.Config); got != want {
		t.Errorf("config:\n got %s\nwant %s", got, want)
	}
	if !res.Accepted || res.WebPending {
		t.Errorf("accepted=%v pending=%v", res.Accepted, res.WebPending)
	}
	if !reflect.DeepEqual(res.Services, []string{"demo"}) {
		t.Errorf("services = %v", res.Services)
	}
	for _, l := range res.Listeners {
		if l.AttachedRoutes != 1 || !l.Valid {
			t.Errorf("listener %s: attached=%d valid=%v", l.Name, l.AttachedRoutes, l.Valid)
		}
	}
	conds := routeConds(t, res, KindHTTPRoute, "demo")
	expectCond(t, conds, "Accepted", metav1.ConditionTrue, "Accepted")
	expectCond(t, conds, "ResolvedRefs", metav1.ConditionTrue, "ResolvedRefs")
	if condOf(conds, "PartiallyInvalid") != nil {
		t.Error("unexpected PartiallyInvalid")
	}
}

func TestSupportedKinds(t *testing.T) {
	gw := gateway(listener("http", gwv1.HTTPProtocolType, 80), listener("https", gwv1.HTTPSProtocolType, 443),
		listener("tls", gwv1.TLSProtocolType, 8443), listener("tcp", gwv1.TCPProtocolType, 5432), listener("udp", gwv1.UDPProtocolType, 53))
	res := Translate(&Input{Gateway: gw, Supported: map[string]bool{KindHTTPRoute: true, KindGRPCRoute: true, KindTCPRoute: true}})
	kinds := map[string][]string{}
	for _, l := range res.Listeners {
		for _, k := range l.SupportedKinds {
			kinds[string(l.Name)] = append(kinds[string(l.Name)], string(k.Kind))
		}
	}
	want := map[string][]string{"http": {"HTTPRoute"}, "https": {"HTTPRoute", "GRPCRoute"}, "tcp": {"TCPRoute"}}
	if !reflect.DeepEqual(kinds, want) {
		t.Errorf("kinds = %v", kinds)
	}
	udp := res.Listeners[4]
	expectCond(t, udp.Conditions, "Accepted", metav1.ConditionFalse, "UnsupportedProtocol")
	if !res.Accepted || res.AcceptedReason != "ListenersNotValid" {
		t.Errorf("gateway accepted = %v %s", res.Accepted, res.AcceptedReason)
	}
}

func TestInvalidRouteKinds(t *testing.T) {
	l := listener("http", gwv1.HTTPProtocolType, 80)
	l.AllowedRoutes = &gwv1.AllowedRoutes{Kinds: []gwv1.RouteGroupKind{{Kind: "TCPRoute"}}}
	res := Translate(&Input{Gateway: gateway(l)})
	expectCond(t, res.Listeners[0].Conditions, "ResolvedRefs", metav1.ConditionFalse, "InvalidRouteKinds")
	if res.Listeners[0].Valid || res.Accepted {
		t.Error("listener with no valid kinds should not be valid")
	}
}

func TestProtocolConflict(t *testing.T) {
	res := Translate(&Input{Gateway: gateway(listener("a", gwv1.HTTPProtocolType, 80), listener("b", gwv1.TCPProtocolType, 80))})
	for _, l := range res.Listeners {
		expectCond(t, l.Conditions, "Conflicted", metav1.ConditionTrue, "ProtocolConflict")
	}
	if res.Accepted {
		t.Error("gateway with only conflicted listeners should not be accepted")
	}
}

func TestNoHostnameUsesGatewayName(t *testing.T) {
	res := Translate(&Input{
		Gateway:       gateway(listener("http", gwv1.HTTPProtocolType, 80)),
		TailnetDomain: domain,
		HTTPRoutes:    []*gwv1.HTTPRoute{httpRoute("infra", "r", 1, nil, rule(backendRef("web", 80)))},
	})
	if _, ok := res.Config.Services["svc:gw"]; !ok {
		t.Errorf("config = %s", mustJSON(t, res.Config))
	}
}

func TestHostnameRules(t *testing.T) {
	for _, tc := range []struct {
		host, label string
		ok          bool
	}{
		{"demo", "demo", true},
		{"Demo", "demo", true},
		{"demo." + domain, "demo", true},
		{"demo.other.ts.net", "demo", true},
		{"demo.example.com", "", false},
		{"*.example.com", "", false},
		{"under_score", "", false},
	} {
		got, err := ServiceLabel(tc.host, domain)
		if (err == nil) != tc.ok || got != tc.label {
			t.Errorf("ServiceLabel(%q) = %q, %v", tc.host, got, err)
		}
	}
	res := Translate(&Input{
		Gateway:       gateway(listener("http", gwv1.HTTPProtocolType, 80)),
		TailnetDomain: domain,
		HTTPRoutes:    []*gwv1.HTTPRoute{httpRoute("infra", "r", 1, []string{"demo.example.com"}, rule(backendRef("web", 80)))},
	})
	expectCond(t, routeConds(t, res, KindHTTPRoute, "r"), "Accepted", metav1.ConditionFalse, "UnsupportedValue")
	if len(res.Config.Services) != 0 {
		t.Errorf("config = %s", mustJSON(t, res.Config))
	}
}

func TestListenerHostnameIntersection(t *testing.T) {
	l := listener("http", gwv1.HTTPProtocolType, 80)
	l.Hostname = ptr.To(gwv1.Hostname("app"))
	res := Translate(&Input{
		Gateway:       gateway(l),
		TailnetDomain: domain,
		HTTPRoutes: []*gwv1.HTTPRoute{
			httpRoute("infra", "match", 1, []string{"app", "other"}, rule(backendRef("web", 80))),
			httpRoute("infra", "nomatch", 2, []string{"other"}, rule(backendRef("web", 80))),
			httpRoute("infra", "none", 3, nil, rule(backendRef("web", 80), prefix("/x"))),
		},
	})
	expectCond(t, routeConds(t, res, KindHTTPRoute, "match"), "Accepted", metav1.ConditionTrue, "Accepted")
	expectCond(t, routeConds(t, res, KindHTTPRoute, "nomatch"), "Accepted", metav1.ConditionFalse, "NoMatchingListenerHostname")
	expectCond(t, routeConds(t, res, KindHTTPRoute, "none"), "Accepted", metav1.ConditionTrue, "Accepted")
	if got := res.Config.ServiceNames(); !reflect.DeepEqual(got, []string{"svc:app"}) {
		t.Errorf("services = %v", got)
	}
	if res.Listeners[0].AttachedRoutes != 2 {
		t.Errorf("attached = %d", res.Listeners[0].AttachedRoutes)
	}
}

func TestUnsupportedMatchesAndFilters(t *testing.T) {
	exact := gwv1.HTTPRouteMatch{Path: &gwv1.HTTPPathMatch{Type: ptr.To(gwv1.PathMatchExact), Value: ptr.To("/exact")}}
	header := gwv1.HTTPRouteMatch{Headers: []gwv1.HTTPHeaderMatch{{Name: "x", Value: "y"}}}
	filtered := rule(backendRef("web", 80), prefix("/f"))
	filtered.Filters = []gwv1.HTTPRouteFilter{{Type: gwv1.HTTPRouteFilterRequestHeaderModifier}}
	res := Translate(&Input{
		Gateway:       gateway(listener("http", gwv1.HTTPProtocolType, 80)),
		TailnetDomain: domain,
		HTTPRoutes: []*gwv1.HTTPRoute{
			httpRoute("infra", "partial", 1, []string{"a"}, rule(backendRef("web", 80), exact, header, prefix("/ok")), filtered),
			httpRoute("infra", "none", 2, []string{"b"}, rule(backendRef("web", 80), exact)),
		},
	})
	conds := routeConds(t, res, KindHTTPRoute, "partial")
	expectCond(t, conds, "Accepted", metav1.ConditionTrue, "Accepted")
	expectCond(t, conds, "PartiallyInvalid", metav1.ConditionTrue, "UnsupportedValue")
	msg := condOf(conds, "PartiallyInvalid").Message
	for _, want := range []string{"Exact", "header", "filters"} {
		if !strings.Contains(msg, want) {
			t.Errorf("message %q lacks %q", msg, want)
		}
	}
	expectCond(t, routeConds(t, res, KindHTTPRoute, "none"), "Accepted", metav1.ConditionFalse, "UnsupportedValue")
	handlers := res.Config.Services["svc:a"].Web["a."+domain+":80"].Handlers
	if len(handlers) != 1 || handlers["/ok"] == nil {
		t.Errorf("handlers = %+v", handlers)
	}
}

func TestBackends(t *testing.T) {
	two := gwv1.HTTPRouteRule{BackendRefs: []gwv1.HTTPBackendRef{{BackendRef: backendRef("a", 80)}, {BackendRef: backendRef("b", 80)}}}
	weighted := two.DeepCopy()
	weighted.BackendRefs[1].Weight = ptr.To(int32(0))
	weighted.Matches = []gwv1.HTTPRouteMatch{prefix("/w")}
	notSvc := backendRef("x", 80)
	notSvc.Kind = ptr.To(gwv1.Kind("ConfigMap"))
	res := Translate(&Input{
		Gateway:       gateway(listener("http", gwv1.HTTPProtocolType, 80)),
		TailnetDomain: domain,
		GetService: func(ns, name string) *corev1.Service {
			if name == "missing" {
				return nil
			}
			return &corev1.Service{}
		},
		HTTPRoutes: []*gwv1.HTTPRoute{
			httpRoute("infra", "split", 1, []string{"split"}, two, *weighted),
			httpRoute("infra", "missing", 2, []string{"missing"}, rule(backendRef("missing", 80))),
			httpRoute("infra", "kind", 3, []string{"kind"}, rule(notSvc)),
		},
	})
	conds := routeConds(t, res, KindHTTPRoute, "split")
	expectCond(t, conds, "Accepted", metav1.ConditionTrue, "Accepted")
	expectCond(t, conds, "PartiallyInvalid", metav1.ConditionTrue, "")
	if h := res.Config.Services["svc:split"].Web["split."+domain+":80"].Handlers; len(h) != 1 || h["/w"].Proxy != "http://a.infra.svc.cluster.local:80/w" {
		t.Errorf("handlers = %s", mustJSON(t, res.Config))
	}
	conds = routeConds(t, res, KindHTTPRoute, "missing")
	expectCond(t, conds, "Accepted", metav1.ConditionTrue, "Accepted")
	expectCond(t, conds, "ResolvedRefs", metav1.ConditionFalse, "BackendNotFound")
	expectCond(t, routeConds(t, res, KindHTTPRoute, "kind"), "ResolvedRefs", metav1.ConditionFalse, "InvalidKind")
}

func TestCrossNamespace(t *testing.T) {
	be := backendRef("web", 80)
	be.Namespace = ptr.To(gwv1.Namespace("backend"))
	mk := func() *gwv1.HTTPRoute {
		return httpRoute("apps", "r", 1, []string{"x"}, rule(be))
	}
	gw := gateway(allNamespaces(listener("http", gwv1.HTTPProtocolType, 80)))

	res := Translate(&Input{Gateway: gw, TailnetDomain: domain, HTTPRoutes: []*gwv1.HTTPRoute{mk()}})
	expectCond(t, routeConds(t, res, KindHTTPRoute, "r"), "ResolvedRefs", metav1.ConditionFalse, "RefNotPermitted")
	if len(res.Config.Services) != 0 {
		t.Error("no service expected without a grant")
	}

	grant := &gwv1.ReferenceGrant{
		ObjectMeta: metav1.ObjectMeta{Name: "g", Namespace: "backend"},
		Spec: gwv1.ReferenceGrantSpec{
			From: []gwv1.ReferenceGrantFrom{{Group: gwv1.GroupName, Kind: "HTTPRoute", Namespace: "apps"}},
			To:   []gwv1.ReferenceGrantTo{{Group: "", Kind: "Service"}},
		},
	}
	res = Translate(&Input{Gateway: gw, TailnetDomain: domain, HTTPRoutes: []*gwv1.HTTPRoute{mk()}, ReferenceGrants: []*gwv1.ReferenceGrant{grant}})
	expectCond(t, routeConds(t, res, KindHTTPRoute, "r"), "ResolvedRefs", metav1.ConditionTrue, "ResolvedRefs")
	if h := res.Config.Services["svc:x"].Web["x."+domain+":80"].Handlers["/"]; h.Proxy != "http://web.backend.svc.cluster.local:80/" {
		t.Errorf("proxy = %s", h.Proxy)
	}
}

func TestAllowedNamespaces(t *testing.T) {
	sameNS := listener("same", gwv1.HTTPProtocolType, 80)
	sel := listener("sel", gwv1.HTTPProtocolType, 8080)
	from := gwv1.NamespacesFromSelector
	sel.AllowedRoutes = &gwv1.AllowedRoutes{Namespaces: &gwv1.RouteNamespaces{From: &from,
		Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"team": "a"}}}}
	in := &Input{
		Gateway:         gateway(sameNS, sel),
		TailnetDomain:   domain,
		NamespaceLabels: map[string]map[string]string{"team-a": {"team": "a"}, "team-b": {"team": "b"}},
		HTTPRoutes: []*gwv1.HTTPRoute{
			httpRoute("team-a", "a", 1, []string{"a"}, rule(backendRef("web", 80))),
			httpRoute("team-b", "b", 2, []string{"b"}, rule(backendRef("web", 80))),
			httpRoute("infra", "local", 3, []string{"local"}, rule(backendRef("web", 80))),
		},
	}
	res := Translate(in)
	expectCond(t, routeConds(t, res, KindHTTPRoute, "a"), "Accepted", metav1.ConditionTrue, "Accepted")
	expectCond(t, routeConds(t, res, KindHTTPRoute, "b"), "Accepted", metav1.ConditionFalse, "NotAllowedByListeners")
	expectCond(t, routeConds(t, res, KindHTTPRoute, "local"), "Accepted", metav1.ConditionTrue, "Accepted")
	if svc := res.Config.Services["svc:a"]; svc == nil || svc.TCP[8080] == nil || svc.TCP[80] != nil {
		t.Errorf("config = %s", mustJSON(t, res.Config))
	}
	if svc := res.Config.Services["svc:local"]; svc == nil || svc.TCP[80] == nil || svc.TCP[8080] != nil {
		t.Errorf("config = %s", mustJSON(t, res.Config))
	}
}

func TestSectionName(t *testing.T) {
	r := httpRoute("infra", "r", 1, []string{"x"}, rule(backendRef("web", 80)))
	r.Spec.ParentRefs[0].SectionName = ptr.To(gwv1.SectionName("b"))
	missing := httpRoute("infra", "m", 2, []string{"y"}, rule(backendRef("web", 80)))
	missing.Spec.ParentRefs[0].SectionName = ptr.To(gwv1.SectionName("nope"))
	res := Translate(&Input{
		Gateway:       gateway(listener("a", gwv1.HTTPProtocolType, 80), listener("b", gwv1.HTTPProtocolType, 8080)),
		TailnetDomain: domain,
		HTTPRoutes:    []*gwv1.HTTPRoute{r, missing},
	})
	if svc := res.Config.Services["svc:x"]; svc.TCP[8080] == nil || svc.TCP[80] != nil {
		t.Errorf("config = %s", mustJSON(t, res.Config))
	}
	expectCond(t, routeConds(t, res, KindHTTPRoute, "m"), "Accepted", metav1.ConditionFalse, "NoMatchingParent")
}

func TestConflictsOldestWins(t *testing.T) {
	res := Translate(&Input{
		Gateway:       gateway(listener("http", gwv1.HTTPProtocolType, 80)),
		TailnetDomain: domain,
		HTTPRoutes: []*gwv1.HTTPRoute{
			httpRoute("infra", "newer", 20, []string{"app"}, rule(backendRef("new", 80))),
			httpRoute("infra", "older", 10, []string{"app"}, rule(backendRef("old", 80)), rule(backendRef("api", 80), prefix("/api"))),
			httpRoute("infra", "merge", 30, []string{"app"}, rule(backendRef("extra", 80), prefix("/extra"))),
		},
	})
	h := res.Config.Services["svc:app"].Web["app."+domain+":80"].Handlers
	if h["/"].Proxy != "http://old.infra.svc.cluster.local:80/" || h["/extra"] == nil || h["/api"] == nil {
		t.Errorf("handlers = %s", mustJSON(t, res.Config))
	}
	expectCond(t, routeConds(t, res, KindHTTPRoute, "newer"), "Accepted", metav1.ConditionFalse, ReasonConflicted)
	expectCond(t, routeConds(t, res, KindHTTPRoute, "merge"), "Accepted", metav1.ConditionTrue, "Accepted")
}

func TestDomainUnknown(t *testing.T) {
	res := Translate(&Input{
		Gateway: gateway(listener("http", gwv1.HTTPProtocolType, 80), listener("tcp", gwv1.TCPProtocolType, 5432)),
		HTTPRoutes: []*gwv1.HTTPRoute{
			httpRoute("infra", "r", 1, []string{"web"}, rule(backendRef("web", 80))),
		},
		TCPRoutes: []*gwv1.TCPRoute{tcpRoute("db", "", backendRef("pg", 5432))},
	})
	if !res.WebPending {
		t.Error("expected WebPending")
	}
	if got := res.Config.ServiceNames(); !reflect.DeepEqual(got, []string{"svc:db"}) {
		t.Errorf("services in config = %v", got)
	}
	if !reflect.DeepEqual(res.Services, []string{"db", "web"}) {
		t.Errorf("wanted services = %v", res.Services)
	}
}

func tcpRoute(name, section string, be gwv1.BackendRef) *gwv1.TCPRoute {
	p := parent("gw")
	if section != "" {
		p.SectionName = ptr.To(gwv1.SectionName(section))
	}
	return &gwv1.TCPRoute{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "infra", Generation: 1},
		Spec: gwv1.TCPRouteSpec{CommonRouteSpec: gwv1.CommonRouteSpec{ParentRefs: []gwv1.ParentReference{p}},
			Rules: []gwv1.TCPRouteRule{{BackendRefs: []gwv1.BackendRef{be}}}},
	}
}

func TestTCPAndTLS(t *testing.T) {
	pass := listener("pass", gwv1.TLSProtocolType, 8443)
	pass.TLS = &gwv1.ListenerTLSConfig{Mode: ptr.To(gwv1.TLSModePassthrough)}
	term := listener("term", gwv1.TLSProtocolType, 9443)
	term.TLS = &gwv1.ListenerTLSConfig{Mode: ptr.To(gwv1.TLSModeTerminate)}
	shared := tcpRoute("db-5432", "tcp-5432", backendRef("pg", 5432))
	shared.Annotations = map[string]string{config.AnnotationTailscaleService: "db"}
	shared2 := tcpRoute("db-6379", "tcp-6379", backendRef("redis", 6379))
	shared2.Annotations = map[string]string{config.AnnotationTailscaleService: "db"}
	tls := func(name, section string, hosts ...string) *gwv1.TLSRoute {
		p := parent("gw")
		p.SectionName = ptr.To(gwv1.SectionName(section))
		r := &gwv1.TLSRoute{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "infra"},
			Spec: gwv1.TLSRouteSpec{CommonRouteSpec: gwv1.CommonRouteSpec{ParentRefs: []gwv1.ParentReference{p}},
				Rules: []gwv1.TLSRouteRule{{BackendRefs: []gwv1.BackendRef{backendRef("tls-app", 443)}}}},
		}
		for _, h := range hosts {
			r.Spec.Hostnames = append(r.Spec.Hostnames, gwv1.Hostname(h))
		}
		return r
	}
	res := Translate(&Input{
		Gateway:       gateway(listener("tcp-5432", gwv1.TCPProtocolType, 5432), listener("tcp-6379", gwv1.TCPProtocolType, 6379), pass, term),
		TailnetDomain: domain,
		TCPRoutes:     []*gwv1.TCPRoute{shared, shared2},
		TLSRoutes:     []*gwv1.TLSRoute{tls("secure", "pass", "secure", "secure2"), tls("term", "term")},
	})
	want := `{"Services":{` +
		`"svc:db":{"TCP":{"5432":{"TCPForward":"pg.infra.svc.cluster.local:5432"},"6379":{"TCPForward":"redis.infra.svc.cluster.local:6379"}}},` +
		`"svc:secure":{"TCP":{"8443":{"TCPForward":"tls-app.infra.svc.cluster.local:443"}}},` +
		`"svc:secure2":{"TCP":{"8443":{"TCPForward":"tls-app.infra.svc.cluster.local:443"}}},` +
		`"svc:term":{"TCP":{"9443":{"TCPForward":"tls-app.infra.svc.cluster.local:443","TerminateTLS":"term.tail1234.ts.net"}}}}}`
	if got := mustJSON(t, res.Config); got != want {
		t.Errorf("config:\n got %s\nwant %s", got, want)
	}
}

func TestGRPC(t *testing.T) {
	grpc := &gwv1.GRPCRoute{
		ObjectMeta: metav1.ObjectMeta{Name: "g", Namespace: "infra", Generation: 2},
		Spec: gwv1.GRPCRouteSpec{
			CommonRouteSpec: gwv1.CommonRouteSpec{ParentRefs: []gwv1.ParentReference{parent("gw")}},
			Hostnames:       []gwv1.Hostname{"rpc"},
			Rules: []gwv1.GRPCRouteRule{
				{Matches: []gwv1.GRPCRouteMatch{{Method: &gwv1.GRPCMethodMatch{Service: ptr.To("echo.v1.Echo"), Method: ptr.To("Say")}}},
					BackendRefs: []gwv1.GRPCBackendRef{{BackendRef: backendRef("say", 9000)}}},
				{Matches: []gwv1.GRPCRouteMatch{{Method: &gwv1.GRPCMethodMatch{Service: ptr.To("echo.v1.Echo")}}},
					BackendRefs: []gwv1.GRPCBackendRef{{BackendRef: backendRef("echo", 9000)}}},
				{Matches: []gwv1.GRPCRouteMatch{{Headers: []gwv1.GRPCHeaderMatch{{Name: "x", Value: "y"}}}},
					BackendRefs: []gwv1.GRPCBackendRef{{BackendRef: backendRef("hdr", 9000)}}},
				{BackendRefs: []gwv1.GRPCBackendRef{{BackendRef: backendRef("default", 9000)}}},
			},
		},
	}
	res := Translate(&Input{
		Gateway:       gateway(listener("https", gwv1.HTTPSProtocolType, 443)),
		TailnetDomain: domain,
		GRPCRoutes:    []*gwv1.GRPCRoute{grpc},
	})
	want := `{"Services":{"svc:rpc":{"TCP":{"443":{"HTTPS":true}},"Web":{"rpc.tail1234.ts.net:443":{"Handlers":{` +
		`"/":{"Proxy":"http://default.infra.svc.cluster.local:9000/"},` +
		`"/echo.v1.Echo":{"Proxy":"http://echo.infra.svc.cluster.local:9000/echo.v1.Echo"},` +
		`"/echo.v1.Echo/Say":{"Proxy":"http://say.infra.svc.cluster.local:9000/echo.v1.Echo/Say"}}}}}}}`
	if got := mustJSON(t, res.Config); got != want {
		t.Errorf("config:\n got %s\nwant %s", got, want)
	}
	expectCond(t, routeConds(t, res, KindGRPCRoute, "g"), "PartiallyInvalid", metav1.ConditionTrue, "UnsupportedValue")

	// gRPC on a plain HTTP listener is refused with an explanation.
	res = Translate(&Input{Gateway: gateway(listener("http", gwv1.HTTPProtocolType, 80)), TailnetDomain: domain, GRPCRoutes: []*gwv1.GRPCRoute{grpc}})
	conds := routeConds(t, res, KindGRPCRoute, "g")
	expectCond(t, conds, "Accepted", metav1.ConditionFalse, "NotAllowedByListeners")
	if !strings.Contains(condOf(conds, "Accepted").Message, "HTTPS") {
		t.Errorf("message = %q", condOf(conds, "Accepted").Message)
	}
}

func TestUDPRoute(t *testing.T) {
	r := &gwv1.UDPRoute{
		ObjectMeta: metav1.ObjectMeta{Name: "dns", Namespace: "infra"},
		Spec:       gwv1.UDPRouteSpec{CommonRouteSpec: gwv1.CommonRouteSpec{ParentRefs: []gwv1.ParentReference{parent("gw")}}},
	}
	res := Translate(&Input{Gateway: gateway(listener("udp", gwv1.UDPProtocolType, 53)), UDPRoutes: []*gwv1.UDPRoute{r}})
	expectCond(t, routeConds(t, res, KindUDPRoute, "dns"), "Accepted", metav1.ConditionFalse, "UnsupportedValue")
	if res.Accepted {
		t.Error("gateway with only a UDP listener should not be accepted")
	}
}

func TestRoutesForOtherGatewaysIgnored(t *testing.T) {
	r := httpRoute("infra", "r", 1, []string{"x"}, rule(backendRef("web", 80)))
	r.Spec.ParentRefs = []gwv1.ParentReference{parent("other")}
	res := Translate(&Input{Gateway: gateway(listener("http", gwv1.HTTPProtocolType, 80)), HTTPRoutes: []*gwv1.HTTPRoute{r}})
	if len(res.Routes) != 0 {
		t.Errorf("routes = %+v", res.Routes)
	}
}

func TestBackendTLSPolicy(t *testing.T) {
	policy := func(name, hostname string, system bool) *gwv1.BackendTLSPolicy {
		p := &gwv1.BackendTLSPolicy{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "infra"},
			Spec: gwv1.BackendTLSPolicySpec{
				TargetRefs: []gwv1.LocalPolicyTargetReferenceWithSectionName{{LocalPolicyTargetReference: gwv1.LocalPolicyTargetReference{Kind: "Service", Name: "secure"}}},
				Validation: gwv1.BackendTLSPolicyValidation{Hostname: gwv1.PreciseHostname(hostname)},
			},
		}
		if system {
			p.Spec.Validation.WellKnownCACertificates = ptr.To(gwv1.WellKnownCACertificatesSystem)
		} else {
			p.Spec.Validation.CACertificateRefs = []gwv1.LocalObjectReference{{Kind: "ConfigMap", Name: "ca"}}
		}
		return p
	}
	run := func(p *gwv1.BackendTLSPolicy) (string, *Result) {
		res := Translate(&Input{
			Gateway:            gateway(listener("http", gwv1.HTTPProtocolType, 80)),
			TailnetDomain:      domain,
			HTTPRoutes:         []*gwv1.HTTPRoute{httpRoute("infra", "r", 1, []string{"x"}, rule(backendRef("secure", 443)))},
			BackendTLSPolicies: []*gwv1.BackendTLSPolicy{p},
		})
		return res.Config.Services["svc:x"].Web["x."+domain+":80"].Handlers["/"].Proxy, res
	}
	proxy, res := run(policy("verified", "secure.infra.svc.cluster.local", true))
	if proxy != "https://secure.infra.svc.cluster.local:443/" {
		t.Errorf("proxy = %s", proxy)
	}
	if len(res.Policies) != 1 || res.Policies[0].Ref.Name != "verified" {
		t.Errorf("policies = %+v", res.Policies)
	}
	proxy, res = run(policy("custom-ca", "secure.example.com", false))
	if proxy != "https+insecure://secure.infra.svc.cluster.local:443/" {
		t.Errorf("proxy = %s", proxy)
	}
	if !strings.Contains(res.Policies[0].Conditions[0].Message, "not verified") {
		t.Errorf("policy message = %q", res.Policies[0].Conditions[0].Message)
	}
	other := policy("elsewhere", "x", true)
	other.Namespace = "other"
	proxy, _ = run(other)
	if proxy != "http://secure.infra.svc.cluster.local:443/" {
		t.Errorf("policy from another namespace must not apply: %s", proxy)
	}
}
