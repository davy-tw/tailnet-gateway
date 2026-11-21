package translate

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"
)

func allowListeners(gw *gwv1.Gateway, from gwv1.FromNamespaces, sel *metav1.LabelSelector) *gwv1.Gateway {
	gw.Spec.AllowedListeners = &gwv1.AllowedListeners{Namespaces: &gwv1.ListenerNamespaces{From: &from, Selector: sel}}
	return gw
}

func entry(name string, proto gwv1.ProtocolType, port int32, hostname string) gwv1.ListenerEntry {
	e := gwv1.ListenerEntry{Name: gwv1.SectionName(name), Protocol: proto, Port: gwv1.PortNumber(port)}
	if hostname != "" {
		e.Hostname = ptr.To(gwv1.Hostname(hostname))
	}
	return e
}

func listenerSet(ns, name string, created int, entries ...gwv1.ListenerEntry) *gwv1.ListenerSet {
	gwNS := gwv1.Namespace("infra")
	return &gwv1.ListenerSet{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Generation: 7,
			CreationTimestamp: metav1.NewTime(time.Unix(int64(created), 0))},
		Spec: gwv1.ListenerSetSpec{ParentRef: gwv1.ParentGatewayReference{Name: "gw", Namespace: &gwNS}, Listeners: entries},
	}
}

func setParent(ns, name, section string) gwv1.ParentReference {
	k := gwv1.Kind(KindListenerSet)
	n := gwv1.Namespace(ns)
	p := gwv1.ParentReference{Kind: &k, Namespace: &n, Name: gwv1.ObjectName(name)}
	if section != "" {
		p.SectionName = ptr.To(gwv1.SectionName(section))
	}
	return p
}

func setResult(t *testing.T, res *Result, ns, name string) ListenerSetResult {
	t.Helper()
	for _, r := range res.ListenerSets {
		if r.Ref.Namespace == ns && r.Ref.Name == name {
			return r
		}
	}
	t.Fatalf("no result for ListenerSet %s/%s", ns, name)
	return ListenerSetResult{}
}

func setListener(t *testing.T, r ListenerSetResult, name string) ListenerResult {
	t.Helper()
	for _, l := range r.Listeners {
		if string(l.Name) == name {
			return l
		}
	}
	t.Fatalf("ListenerSet %s has no listener %s", r.Ref, name)
	return ListenerResult{}
}

func TestListenerSetNotAllowedByDefault(t *testing.T) {
	res := Translate(&Input{
		Gateway:      gateway(listener("http", gwv1.HTTPProtocolType, 80)),
		ListenerSets: []*gwv1.ListenerSet{listenerSet("infra", "ls", 1, entry("extra", gwv1.HTTPProtocolType, 8080, ""))},
	})
	r := setResult(t, res, "infra", "ls")
	if r.Accepted || r.Reason != string(gwv1.ListenerSetReasonNotAllowed) || len(r.Listeners) != 0 {
		t.Errorf("result = %+v", r)
	}
	if res.AttachedListenerSets != 0 || len(res.Listeners) != 1 {
		t.Errorf("attached = %d, listeners = %d", res.AttachedListenerSets, len(res.Listeners))
	}
}

func TestListenerSetAllowedNamespaces(t *testing.T) {
	sel := &metav1.LabelSelector{MatchLabels: map[string]string{"gateway": "shared"}}
	sets := []*gwv1.ListenerSet{
		listenerSet("infra", "local", 1, entry("a", gwv1.HTTPProtocolType, 80, "a")),
		listenerSet("team-a", "a", 2, entry("b", gwv1.HTTPProtocolType, 80, "b")),
		listenerSet("team-b", "b", 3, entry("c", gwv1.HTTPProtocolType, 80, "c")),
	}
	labels := map[string]map[string]string{"team-a": {"gateway": "shared"}}
	for _, tc := range []struct {
		from gwv1.FromNamespaces
		want map[string]bool
	}{
		{gwv1.NamespacesFromNone, map[string]bool{"local": false, "a": false, "b": false}},
		{gwv1.NamespacesFromSame, map[string]bool{"local": true, "a": false, "b": false}},
		{gwv1.NamespacesFromSelector, map[string]bool{"local": false, "a": true, "b": false}},
		{gwv1.NamespacesFromAll, map[string]bool{"local": true, "a": true, "b": true}},
	} {
		gw := allowListeners(gateway(listener("http", gwv1.HTTPProtocolType, 80)), tc.from, sel)
		res := Translate(&Input{Gateway: gw, ListenerSets: sets, NamespaceLabels: labels})
		n := int32(0)
		for _, r := range res.ListenerSets {
			if r.Accepted != tc.want[r.Ref.Name] {
				t.Errorf("from %s: %s accepted = %v (%s)", tc.from, r.Ref, r.Accepted, r.Message)
			}
			if r.Accepted {
				n++
			}
		}
		if res.AttachedListenerSets != n {
			t.Errorf("from %s: attached = %d, want %d", tc.from, res.AttachedListenerSets, n)
		}
	}
}

func TestListenerSetParentRefMustNameGateway(t *testing.T) {
	other := listenerSet("infra", "other", 1, entry("a", gwv1.HTTPProtocolType, 80, ""))
	other.Spec.ParentRef.Name = "elsewhere"
	defaulted := listenerSet("infra", "defaulted", 2, entry("b", gwv1.HTTPProtocolType, 8080, ""))
	defaulted.Spec.ParentRef.Namespace = nil // the ListenerSet's own namespace
	res := Translate(&Input{
		Gateway:      allowListeners(gateway(listener("http", gwv1.HTTPProtocolType, 80)), gwv1.NamespacesFromSame, nil),
		ListenerSets: []*gwv1.ListenerSet{other, defaulted},
	})
	if len(res.ListenerSets) != 1 || res.ListenerSets[0].Ref.Name != "defaulted" {
		t.Errorf("results = %+v", res.ListenerSets)
	}
}

// TestListenerSetRouting checks that ListenerSet listeners are served by
// the Gateway's proxy, that routes attach to them only through a
// ListenerSet parentRef, and that listener statuses go to the right
// object.
func TestListenerSetRouting(t *testing.T) {
	ls := listenerSet("team-a", "apps", 1,
		entry("web", gwv1.HTTPProtocolType, 80, "web"),
		entry("secure", gwv1.HTTPSProtocolType, 443, "secure"))
	viaSet := httpRoute("team-a", "via-set", 1, nil, rule(backendRef("web", 80)))
	viaSet.Spec.ParentRefs = []gwv1.ParentReference{setParent("team-a", "apps", "web")}
	viaSetAll := httpRoute("team-a", "via-set-all", 2, []string{"secure"}, rule(backendRef("api", 80), prefix("/api")))
	viaSetAll.Spec.ParentRefs = []gwv1.ParentReference{setParent("team-a", "apps", "")}
	// A parentRef to the Gateway does not reach ListenerSet listeners.
	viaGateway := httpRoute("team-a", "via-gateway", 3, nil, rule(backendRef("web", 80)))
	viaGateway.Spec.ParentRefs[0].SectionName = ptr.To(gwv1.SectionName("web"))
	// The same name on the Gateway is a different listener.
	own := httpRoute("infra", "own", 4, []string{"own"}, rule(backendRef("own", 80)))
	own.Spec.ParentRefs[0].SectionName = ptr.To(gwv1.SectionName("web"))

	res := Translate(&Input{
		Gateway:       allowListeners(gateway(listener("web", gwv1.HTTPProtocolType, 8080)), gwv1.NamespacesFromAll, nil),
		TailnetDomain: domain,
		ListenerSets:  []*gwv1.ListenerSet{ls},
		HTTPRoutes:    []*gwv1.HTTPRoute{viaSet, viaSetAll, viaGateway, own},
	})
	if svc := res.Config.Services["svc:web"]; svc == nil || svc.TCP[80] == nil || !svc.TCP[80].HTTP {
		t.Errorf("config = %s", mustJSON(t, res.Config))
	}
	if svc := res.Config.Services["svc:secure"]; svc == nil || svc.TCP[443] == nil || !svc.TCP[443].HTTPS ||
		svc.Web["secure."+domain+":443"].Handlers["/api"] == nil {
		t.Errorf("config = %s", mustJSON(t, res.Config))
	}
	if svc := res.Config.Services["svc:own"]; svc == nil || svc.TCP[8080] == nil {
		t.Errorf("config = %s", mustJSON(t, res.Config))
	}
	expectCond(t, routeConds(t, res, KindHTTPRoute, "via-set"), "Accepted", metav1.ConditionTrue, "Accepted")
	expectCond(t, routeConds(t, res, KindHTTPRoute, "via-set-all"), "Accepted", metav1.ConditionTrue, "Accepted")
	// team-a is not the Gateway's namespace: its own listener allows Same only.
	expectCond(t, routeConds(t, res, KindHTTPRoute, "via-gateway"), "Accepted", metav1.ConditionFalse, "NotAllowedByListeners")
	expectCond(t, routeConds(t, res, KindHTTPRoute, "own"), "Accepted", metav1.ConditionTrue, "Accepted")

	if len(res.Listeners) != 1 || res.Listeners[0].AttachedRoutes != 1 {
		t.Errorf("gateway listeners = %+v", res.Listeners)
	}
	r := setResult(t, res, "team-a", "apps")
	if !r.Accepted || r.Reason != "Accepted" || res.AttachedListenerSets != 1 {
		t.Errorf("result = %+v, attached = %d", r, res.AttachedListenerSets)
	}
	if web := setListener(t, r, "web"); web.AttachedRoutes != 1 || !web.Valid { // via-set-all only matches "secure"
		t.Errorf("web = %+v", web)
	}
	if secure := setListener(t, r, "secure"); secure.AttachedRoutes != 1 {
		t.Errorf("secure = %+v", secure)
	}
	for _, c := range setListener(t, r, "web").Conditions {
		if c.ObservedGeneration != 7 {
			t.Errorf("%s observedGeneration = %d, want the ListenerSet's", c.Type, c.ObservedGeneration)
		}
	}
	if !equalStrings(res.Services, []string{"own", "secure", "web"}) {
		t.Errorf("services = %v", res.Services)
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestListenerSetAllowedRoutesSame checks that "Same" on a ListenerSet
// listener means the ListenerSet's namespace.
func TestListenerSetAllowedRoutesSame(t *testing.T) {
	ls := listenerSet("team-a", "apps", 1, entry("web", gwv1.HTTPProtocolType, 80, ""))
	inSet := httpRoute("team-a", "in-set-ns", 1, []string{"a"}, rule(backendRef("web", 80)))
	inSet.Spec.ParentRefs = []gwv1.ParentReference{setParent("team-a", "apps", "")}
	inGW := httpRoute("infra", "in-gateway-ns", 2, []string{"b"}, rule(backendRef("web", 80)))
	inGW.Spec.ParentRefs = []gwv1.ParentReference{setParent("team-a", "apps", "")}
	res := Translate(&Input{
		Gateway:       allowListeners(gateway(listener("http", gwv1.HTTPProtocolType, 8080)), gwv1.NamespacesFromAll, nil),
		TailnetDomain: domain,
		ListenerSets:  []*gwv1.ListenerSet{ls},
		HTTPRoutes:    []*gwv1.HTTPRoute{inSet, inGW},
	})
	expectCond(t, routeConds(t, res, KindHTTPRoute, "in-set-ns"), "Accepted", metav1.ConditionTrue, "Accepted")
	expectCond(t, routeConds(t, res, KindHTTPRoute, "in-gateway-ns"), "Accepted", metav1.ConditionFalse, "NotAllowedByListeners")
}

func TestListenerSetRouteToDisallowedSet(t *testing.T) {
	ls := listenerSet("team-a", "apps", 1, entry("web", gwv1.HTTPProtocolType, 80, ""))
	r := httpRoute("team-a", "r", 1, []string{"a"}, rule(backendRef("web", 80)))
	r.Spec.ParentRefs = []gwv1.ParentReference{setParent("team-a", "apps", "")}
	res := Translate(&Input{
		Gateway:       gateway(listener("http", gwv1.HTTPProtocolType, 8080)),
		TailnetDomain: domain,
		ListenerSets:  []*gwv1.ListenerSet{ls},
		HTTPRoutes:    []*gwv1.HTTPRoute{r},
	})
	expectCond(t, routeConds(t, res, KindHTTPRoute, "r"), "Accepted", metav1.ConditionFalse, "NotAllowedByListeners")
	if len(res.Config.Services) != 0 {
		t.Errorf("config = %s", mustJSON(t, res.Config))
	}
}

// TestListenerSetConflicts follows the conformance scenarios: the Gateway
// wins over ListenerSets, older ListenerSets over newer ones, and a
// ListenerSet whose listeners all conflict is not accepted.
func TestListenerSetConflicts(t *testing.T) {
	for _, tc := range []struct {
		name     string
		proto    gwv1.ProtocolType
		hostname string
		reason   string
	}{
		{"hostname", gwv1.HTTPProtocolType, "taken", string(gwv1.ListenerReasonHostnameConflict)},
		{"protocol", gwv1.TCPProtocolType, "", string(gwv1.ListenerReasonProtocolConflict)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gwListener := listener("gw", gwv1.HTTPProtocolType, 80)
			gwListener.Hostname = ptr.To(gwv1.Hostname("taken"))
			gw := allowListeners(gateway(gwListener, listener("gw-other", gwv1.HTTPProtocolType, 80)), gwv1.NamespacesFromSame, nil)
			gw.Spec.Listeners[1].Hostname = ptr.To(gwv1.Hostname("other"))
			older := listenerSet("infra", "older", 1,
				entry("ok", gwv1.HTTPProtocolType, 80, "older-ok"),
				entry("vs-gateway", tc.proto, 80, tc.hostname),
				entry("vs-set", gwv1.HTTPProtocolType, 80, "shared"))
			newerFirst := listenerSet("infra", "newer", 2,
				entry("ok", gwv1.HTTPProtocolType, 80, "newer-ok"),
				entry("vs-set", tc.proto, 80, map[string]string{"hostname": "shared"}[tc.name]))
			onlyConflict := listenerSet("infra", "only", 3, entry("vs-gateway", tc.proto, 80, tc.hostname))
			res := Translate(&Input{Gateway: gw, ListenerSets: []*gwv1.ListenerSet{onlyConflict, newerFirst, older}})

			for _, l := range res.Listeners {
				if !l.Valid {
					t.Errorf("gateway listener %s not valid: %+v", l.Name, l.Conditions)
				}
			}
			conflicted := func(l ListenerResult) {
				t.Helper()
				expectCond(t, l.Conditions, "Accepted", metav1.ConditionFalse, tc.reason)
				expectCond(t, l.Conditions, "Conflicted", metav1.ConditionTrue, tc.reason)
				if l.Valid || l.InvalidReason != tc.reason {
					t.Errorf("listener %s valid=%v invalidReason=%s", l.Name, l.Valid, l.InvalidReason)
				}
			}
			fine := func(l ListenerResult) {
				t.Helper()
				expectCond(t, l.Conditions, "Accepted", metav1.ConditionTrue, "")
				expectCond(t, l.Conditions, "Conflicted", metav1.ConditionFalse, "NoConflicts")
			}
			o := setResult(t, res, "infra", "older")
			fine(setListener(t, o, "ok"))
			conflicted(setListener(t, o, "vs-gateway"))
			fine(setListener(t, o, "vs-set"))
			if !o.Accepted || o.Reason != string(gwv1.ListenerSetReasonListenersNotValid) {
				t.Errorf("older = %+v", o)
			}
			n := setResult(t, res, "infra", "newer")
			fine(setListener(t, n, "ok"))
			conflicted(setListener(t, n, "vs-set"))
			if !n.Accepted {
				t.Errorf("newer = %+v", n)
			}
			only := setResult(t, res, "infra", "only")
			conflicted(setListener(t, only, "vs-gateway"))
			if only.Accepted || only.Reason != string(gwv1.ListenerSetReasonListenersNotValid) {
				t.Errorf("only = %+v", only)
			}
			if res.AttachedListenerSets != 2 || !res.Accepted {
				t.Errorf("attached = %d, accepted = %v", res.AttachedListenerSets, res.Accepted)
			}
			// Precedence order in the results.
			if res.ListenerSets[0].Ref.Name != "older" || res.ListenerSets[2].Ref.Name != "only" {
				t.Errorf("order = %v, %v, %v", res.ListenerSets[0].Ref, res.ListenerSets[1].Ref, res.ListenerSets[2].Ref)
			}
		})
	}
}

func TestListenerSetOfUnacceptedGateway(t *testing.T) {
	gw := allowListeners(gateway(listener("udp", gwv1.UDPProtocolType, 53)), gwv1.NamespacesFromSame, nil)
	res := Translate(&Input{Gateway: gw, ListenerSets: []*gwv1.ListenerSet{listenerSet("infra", "ls", 1, entry("web", gwv1.HTTPProtocolType, 80, ""))}})
	r := setResult(t, res, "infra", "ls")
	if res.Accepted || r.Accepted || r.Reason != string(gwv1.ListenerSetReasonParentNotAccepted) || res.AttachedListenerSets != 0 {
		t.Errorf("gateway accepted=%v, result = %+v, attached = %d", res.Accepted, r, res.AttachedListenerSets)
	}
}

func TestGatewayWithOnlyListenerSets(t *testing.T) {
	gw := allowListeners(gateway(), gwv1.NamespacesFromSame, nil)
	r := httpRoute("infra", "r", 1, []string{"app"}, rule(backendRef("web", 80)))
	r.Spec.ParentRefs = []gwv1.ParentReference{setParent("infra", "ls", "")}
	res := Translate(&Input{
		Gateway: gw, TailnetDomain: domain,
		ListenerSets: []*gwv1.ListenerSet{listenerSet("infra", "ls", 1, entry("web", gwv1.HTTPProtocolType, 80, ""))},
		HTTPRoutes:   []*gwv1.HTTPRoute{r},
	})
	if !res.Accepted || res.AttachedListenerSets != 1 || res.Config.Services["svc:app"] == nil {
		t.Errorf("accepted=%v attached=%d config=%s", res.Accepted, res.AttachedListenerSets, mustJSON(t, res.Config))
	}
}

func TestListenerSetTCPRoute(t *testing.T) {
	tr := tcpRoute("db", "", backendRef("postgres", 5432))
	tr.Namespace = "team-a"
	tr.Spec.ParentRefs = []gwv1.ParentReference{setParent("team-a", "apps", "pg")}
	res := Translate(&Input{
		Gateway:       allowListeners(gateway(listener("http", gwv1.HTTPProtocolType, 80)), gwv1.NamespacesFromAll, nil),
		TailnetDomain: domain,
		ListenerSets:  []*gwv1.ListenerSet{listenerSet("team-a", "apps", 1, entry("pg", gwv1.TCPProtocolType, 5432, ""))},
		TCPRoutes:     []*gwv1.TCPRoute{tr},
	})
	if svc := res.Config.Services["svc:db"]; svc == nil || svc.TCP[5432] == nil || svc.TCP[5432].TCPForward != "postgres.team-a.svc.cluster.local:5432" {
		t.Errorf("config = %s", mustJSON(t, res.Config))
	}
}

// TestListenerSetTLS covers the HTTPS and TLS passthrough listeners of
// examples/listenerset.yaml.
func TestListenerSetTLS(t *testing.T) {
	secure := entry("secure", gwv1.HTTPSProtocolType, 443, "")
	secure.TLS = &gwv1.ListenerTLSConfig{Mode: ptr.To(gwv1.TLSModeTerminate)}
	pass := entry("passthrough", gwv1.TLSProtocolType, 8443, "")
	pass.TLS = &gwv1.ListenerTLSConfig{Mode: ptr.To(gwv1.TLSModePassthrough)}
	hr := httpRoute("infra", "team-secure", 1, []string{"team-secure"}, rule(backendRef("web", 80)))
	hr.Spec.ParentRefs = []gwv1.ParentReference{setParent("infra", "team", "secure")}
	tr := &gwv1.TLSRoute{
		ObjectMeta: metav1.ObjectMeta{Name: "team-tls", Namespace: "infra", Generation: 1},
		Spec: gwv1.TLSRouteSpec{CommonRouteSpec: gwv1.CommonRouteSpec{ParentRefs: []gwv1.ParentReference{setParent("infra", "team", "passthrough")}},
			Hostnames: []gwv1.Hostname{"team-tls"}, Rules: []gwv1.TLSRouteRule{{BackendRefs: []gwv1.BackendRef{backendRef("web", 443)}}}},
	}
	res := Translate(&Input{
		Gateway:       allowListeners(gateway(listener("http", gwv1.HTTPProtocolType, 80)), gwv1.NamespacesFromSame, nil),
		TailnetDomain: domain,
		ListenerSets:  []*gwv1.ListenerSet{listenerSet("infra", "team", 1, secure, pass)},
		HTTPRoutes:    []*gwv1.HTTPRoute{hr},
		TLSRoutes:     []*gwv1.TLSRoute{tr},
	})
	if svc := res.Config.Services["svc:team-secure"]; svc == nil || svc.TCP[443] == nil || !svc.TCP[443].HTTPS {
		t.Errorf("config = %s", mustJSON(t, res.Config))
	}
	if svc := res.Config.Services["svc:team-tls"]; svc == nil || svc.TCP[8443] == nil ||
		svc.TCP[8443].TCPForward != "web.infra.svc.cluster.local:443" || svc.TCP[8443].TerminateTLS != "" {
		t.Errorf("config = %s", mustJSON(t, res.Config))
	}
	expectCond(t, routeConds(t, res, KindTLSRoute, "team-tls"), "Accepted", metav1.ConditionTrue, "Accepted")
}

// tenantGateway is a Gateway owned by "infra" whose listener admin
// (HTTP :80, hostname admin) serves the infra route, and which allows
// ListenerSets from every namespace.
func tenantGateway() (*gwv1.Gateway, *gwv1.HTTPRoute) {
	l := listener("admin", gwv1.HTTPProtocolType, 80)
	l.Hostname = ptr.To(gwv1.Hostname("admin"))
	gw := allowListeners(gateway(l), gwv1.NamespacesFromAll, nil)
	infra := httpRoute("infra", "admin", 10, nil, rule(backendRef("admin", 80)))
	return gw, infra
}

func tenantRoute(name string, created int, hostnames []string, set, section string) *gwv1.HTTPRoute {
	r := httpRoute("team", name, created, hostnames, rule(backendRef("evil", 80)))
	r.Spec.ParentRefs = []gwv1.ParentReference{setParent("team", set, section)}
	return r
}

// A ListenerSet listener without a hostname on the Gateway's port must
// not let an older tenant route take the Gateway listener's Service.
func TestListenerSetCatchAllCannotTakeGatewayService(t *testing.T) {
	gw, infra := tenantGateway()
	res := Translate(&Input{
		Gateway: gw, TailnetDomain: domain,
		ListenerSets: []*gwv1.ListenerSet{listenerSet("team", "evil", 1, entry("any", gwv1.HTTPProtocolType, 80, ""))},
		HTTPRoutes:   []*gwv1.HTTPRoute{infra, tenantRoute("evil", 1, []string{"admin"}, "evil", "")},
	})
	expectCond(t, routeConds(t, res, KindHTTPRoute, "admin"), "Accepted", metav1.ConditionTrue, "Accepted")
	expectCond(t, routeConds(t, res, KindHTTPRoute, "evil"), "Accepted", metav1.ConditionFalse, ReasonConflicted)
	h := res.Config.Services["svc:admin"].Web["admin."+domain+":80"].Handlers
	if h["/"].Proxy != "http://admin.infra.svc.cluster.local:80/" {
		t.Errorf("config = %s", mustJSON(t, res.Config))
	}
}

// Listener hostnames are compared by the Tailscale Service they name, on
// every port.
func TestListenerSetHostnameNamesGatewayService(t *testing.T) {
	for _, tc := range []struct {
		name string
		e    gwv1.ListenerEntry
	}{
		{"tailnet FQDN on the same port", entry("x", gwv1.HTTPProtocolType, 80, "admin."+domain)},
		{"trailing dot", entry("x", gwv1.HTTPProtocolType, 80, "admin."+domain+".")},
		{"another port", entry("x", gwv1.HTTPProtocolType, 8080, "admin")},
		{"another protocol", entry("x", gwv1.TLSProtocolType, 8443, "admin")},
		{"the Gateway's default Service", entry("x", gwv1.HTTPProtocolType, 9000, "gw")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gw, infra := tenantGateway()
			if tc.e.Protocol == gwv1.TLSProtocolType {
				tc.e.TLS = &gwv1.ListenerTLSConfig{Mode: ptr.To(gwv1.TLSModePassthrough)}
			}
			res := Translate(&Input{
				Gateway: gw, TailnetDomain: domain,
				ListenerSets: []*gwv1.ListenerSet{listenerSet("team", "evil", 1, tc.e)},
				HTTPRoutes:   []*gwv1.HTTPRoute{infra, tenantRoute("evil", 1, nil, "evil", "")},
			})
			r := setResult(t, res, "team", "evil")
			x := setListener(t, r, "x")
			expectCond(t, x.Conditions, "Accepted", metav1.ConditionFalse, "HostnameConflict")
			expectCond(t, x.Conditions, "Conflicted", metav1.ConditionTrue, "HostnameConflict")
			if r.Accepted {
				t.Errorf("ListenerSet accepted: %+v", r)
			}
			for port := range res.Config.Services["svc:admin"].TCP {
				if port != 80 {
					t.Errorf("svc:admin gained port %d: %s", port, mustJSON(t, res.Config))
				}
			}
		})
	}
}

// A route without hostnames on a ListenerSet listener without a hostname
// has no Service of its own, and must not fall back to the Gateway's.
func TestListenerSetHostlessRoute(t *testing.T) {
	gw, _ := tenantGateway()
	res := Translate(&Input{
		Gateway: gw, TailnetDomain: domain,
		ListenerSets: []*gwv1.ListenerSet{listenerSet("team", "apps", 1, entry("any", gwv1.HTTPProtocolType, 9000, ""))},
		HTTPRoutes:   []*gwv1.HTTPRoute{tenantRoute("hostless", 1, nil, "apps", "")},
	})
	expectCond(t, routeConds(t, res, KindHTTPRoute, "hostless"), "Accepted", metav1.ConditionFalse, "UnsupportedValue")
	if len(res.Config.Services) != 0 {
		t.Errorf("config = %s", mustJSON(t, res.Config))
	}
	if l := setListener(t, setResult(t, res, "team", "apps"), "any"); l.AttachedRoutes != 0 {
		t.Errorf("attachedRoutes = %d", l.AttachedRoutes)
	}
}

// Services the Gateway's routes use on a listener without a hostname
// belong to the Gateway too, and a later ListenerSet cannot take them,
// neither with a listener hostname nor with route hostnames.
func TestListenerSetCannotTakeServiceOfGatewayRoute(t *testing.T) {
	gw := allowListeners(gateway(listener("http", gwv1.HTTPProtocolType, 80)), gwv1.NamespacesFromAll, nil)
	infra := httpRoute("infra", "shop", 10, []string{"shop"}, rule(backendRef("shop", 80)))
	res := Translate(&Input{
		Gateway: gw, TailnetDomain: domain,
		ListenerSets: []*gwv1.ListenerSet{
			listenerSet("team", "named", 1, entry("x", gwv1.HTTPProtocolType, 8080, "shop")),
			listenerSet("team", "open", 2, entry("y", gwv1.HTTPProtocolType, 8081, "")),
		},
		HTTPRoutes: []*gwv1.HTTPRoute{infra, tenantRoute("evil", 1, []string{"shop"}, "open", "")},
	})
	expectCond(t, setListener(t, setResult(t, res, "team", "named"), "x").Conditions, "Accepted", metav1.ConditionFalse, "HostnameConflict")
	expectCond(t, routeConds(t, res, KindHTTPRoute, "evil"), "Accepted", metav1.ConditionFalse, ReasonConflicted)
	expectCond(t, routeConds(t, res, KindHTTPRoute, "shop"), "Accepted", metav1.ConditionTrue, "Accepted")
	if svc := res.Config.Services["svc:shop"]; len(svc.TCP) != 1 || svc.TCP[80] == nil {
		t.Errorf("config = %s", mustJSON(t, res.Config))
	}
}

// Between ListenerSets, the older ListenerSet keeps a Service its routes
// use, even against an older route of a newer ListenerSet.
func TestListenerSetServiceBelongsToOlderSet(t *testing.T) {
	gw := allowListeners(gateway(listener("http", gwv1.HTTPProtocolType, 80)), gwv1.NamespacesFromAll, nil)
	first := tenantRoute("first", 20, []string{"app"}, "older", "")
	second := tenantRoute("second", 10, []string{"app"}, "newer", "")
	res := Translate(&Input{
		Gateway: gw, TailnetDomain: domain,
		ListenerSets: []*gwv1.ListenerSet{
			listenerSet("team", "older", 1, entry("a", gwv1.HTTPProtocolType, 8080, "")),
			listenerSet("team", "newer", 2, entry("b", gwv1.HTTPProtocolType, 8081, "")),
		},
		HTTPRoutes: []*gwv1.HTTPRoute{first, second},
	})
	expectCond(t, routeConds(t, res, KindHTTPRoute, "first"), "Accepted", metav1.ConditionTrue, "Accepted")
	expectCond(t, routeConds(t, res, KindHTTPRoute, "second"), "Accepted", metav1.ConditionFalse, ReasonConflicted)
	if svc := res.Config.Services["svc:app"]; len(svc.TCP) != 1 || svc.TCP[8080] == nil {
		t.Errorf("config = %s", mustJSON(t, res.Config))
	}
}

// The Gateway's own conflicting listeners still keep their port.
func TestListenerSetBlockedByConflictedGatewayListener(t *testing.T) {
	gw := allowListeners(gateway(listener("a", gwv1.HTTPProtocolType, 80), listener("b", gwv1.TCPProtocolType, 80)), gwv1.NamespacesFromAll, nil)
	res := Translate(&Input{
		Gateway:      gw,
		ListenerSets: []*gwv1.ListenerSet{listenerSet("team", "ls", 1, entry("x", gwv1.HTTPProtocolType, 80, "x"))},
	})
	expectCond(t, setListener(t, setResult(t, res, "team", "ls"), "x").Conditions, "Conflicted", metav1.ConditionTrue, "ProtocolConflict")
}

// A route with parentRefs to the Gateway and to a ListenerSet keeps its
// parent results in parentRef order.
func TestListenerSetDualParentOrder(t *testing.T) {
	gw := allowListeners(gateway(listener("http", gwv1.HTTPProtocolType, 80)), gwv1.NamespacesFromSame, nil)
	r := httpRoute("infra", "dual", 1, []string{"dual"}, rule(backendRef("web", 80)))
	r.Spec.ParentRefs = []gwv1.ParentReference{setParent("infra", "ls", ""), parent("gw")}
	res := Translate(&Input{
		Gateway: gw, TailnetDomain: domain,
		ListenerSets: []*gwv1.ListenerSet{listenerSet("infra", "ls", 1, entry("x", gwv1.HTTPProtocolType, 8080, ""))},
		HTTPRoutes:   []*gwv1.HTTPRoute{r},
	})
	parents := res.Routes[0].Parents
	if len(parents) != 2 || parents[0].ParentRef.Kind == nil || *parents[0].ParentRef.Kind != KindListenerSet || parents[1].ParentRef.Kind != nil {
		t.Fatalf("parents = %+v", parents)
	}
	for _, p := range parents {
		expectCond(t, p.Conditions, "Accepted", metav1.ConditionTrue, "Accepted")
	}
	if svc := res.Config.Services["svc:dual"]; len(svc.TCP) != 2 {
		t.Errorf("config = %s", mustJSON(t, res.Config))
	}
}
