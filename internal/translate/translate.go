// Package translate turns a Gateway and the routes attached to it into a
// Tailscale serve config, plus the Gateway, listener, route and policy
// status that goes with it. It is pure: all cluster state comes in through
// Input.
package translate

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/davy-tw/tailnet-gateway/internal/config"
	"github.com/davy-tw/tailnet-gateway/internal/serveconf"
)

// Route kinds.
const (
	KindHTTPRoute = "HTTPRoute"
	KindGRPCRoute = "GRPCRoute"
	KindTLSRoute  = "TLSRoute"
	KindTCPRoute  = "TCPRoute"
	KindUDPRoute  = "UDPRoute"
	KindGateway   = "Gateway"
	KindService   = "Service"
)

// ReasonConflicted is an implementation-specific route reason used when a
// route maps onto a Tailscale Service port or path that an older route
// already uses.
const ReasonConflicted = "Conflicted"

// Input is everything the translation needs.
type Input struct {
	Gateway        *gwv1.Gateway
	ControllerName string

	HTTPRoutes []*gwv1.HTTPRoute
	GRPCRoutes []*gwv1.GRPCRoute
	TLSRoutes  []*gwv1.TLSRoute
	TCPRoutes  []*gwv1.TCPRoute
	UDPRoutes  []*gwv1.UDPRoute

	ReferenceGrants    []*gwv1.ReferenceGrant
	BackendTLSPolicies []*gwv1.BackendTLSPolicy

	// NamespaceLabels maps namespace name to its labels, for listener
	// allowedRoutes selectors.
	NamespaceLabels map[string]map[string]string
	// GetService returns a backend Service, or nil if it does not exist.
	// When GetService itself is nil, every backend is assumed to exist.
	GetService func(namespace, name string) *corev1.Service

	ClusterDomain string
	// TailnetDomain is the MagicDNS suffix ("tail1234.ts.net"); empty while
	// unknown.
	TailnetDomain string
	// Supported lists the route kinds whose CRDs are served. A nil map
	// means all kinds are supported.
	Supported map[string]bool
}

// RouteRef identifies a route.
type RouteRef struct {
	Kind      string
	Namespace string
	Name      string
}

func (r RouteRef) String() string { return r.Kind + " " + r.Namespace + "/" + r.Name }

// ParentResult is the status of a route for one parentRef that points at
// this Gateway.
type ParentResult struct {
	ParentRef  gwv1.ParentReference
	Conditions []metav1.Condition
}

// RouteResult is the status of one route with respect to this Gateway.
type RouteResult struct {
	Ref     RouteRef
	Parents []ParentResult
}

// ListenerResult is the status of one listener, without Programmed.
type ListenerResult struct {
	Name           gwv1.SectionName
	SupportedKinds []gwv1.RouteGroupKind
	AttachedRoutes int32
	Conditions     []metav1.Condition
	// Valid listeners can carry traffic.
	Valid bool
}

// PolicyResult is a BackendTLSPolicy's status with respect to this Gateway.
type PolicyResult struct {
	Ref        types.NamespacedName
	Conditions []metav1.Condition
}

// Result is the output of Translate.
type Result struct {
	Config    *serveconf.Config
	Listeners []ListenerResult
	Routes    []RouteResult
	Policies  []PolicyResult
	// Services lists every Tailscale Service label the Gateway wants,
	// including ones left out of Config while the tailnet domain is unknown.
	Services []string
	// WebPending is set when HTTP handlers or TLS termination were left out
	// because the tailnet domain is not known yet.
	WebPending bool
	// AcceptedReason and AcceptedMessage describe the Gateway's Accepted
	// condition; Accepted is false when no listener is valid.
	Accepted        bool
	AcceptedReason  string
	AcceptedMessage string
}

// Translate builds the serve config and statuses.
func Translate(in *Input) *Result {
	t := &translator{
		in:       in,
		services: map[string]*svcState{},
		policies: map[types.NamespacedName]*policyState{},
	}
	return t.run()
}

type translator struct {
	in        *Input
	listeners []*listenerState
	services  map[string]*svcState
	policies  map[types.NamespacedName]*policyState
	routes    []RouteResult
	pending   bool
}

type listenerState struct {
	l      gwv1.Listener
	res    ListenerResult
	kinds  map[string]bool
	family string
}

type svcState struct {
	label string
	ports map[uint16]*portState
}

type portState struct {
	mode   string // "http", "https", "tcp", "tls"
	owner  RouteRef
	target string // tcp and tls modes
	mounts map[string]mount
	order  []string
}

type mount struct {
	proxy string
	owner RouteRef
}

type policyState struct {
	policy  *gwv1.BackendTLSPolicy
	message string
}

func (t *translator) supported(kind string) bool {
	if t.in.Supported == nil {
		return true
	}
	return t.in.Supported[kind]
}

func (t *translator) gen() int64 { return t.in.Gateway.Generation }

func (t *translator) run() *Result {
	t.buildListeners()
	t.processRoutes()

	res := &Result{
		Config:     t.renderConfig(),
		Routes:     t.routes,
		WebPending: t.pending,
	}
	for _, ls := range t.listeners {
		res.Listeners = append(res.Listeners, ls.res)
	}
	for label := range t.services {
		res.Services = append(res.Services, label)
	}
	sort.Strings(res.Services)
	res.Policies = t.policyResults()

	valid := 0
	for _, ls := range t.listeners {
		if ls.res.Valid {
			valid++
		}
	}
	switch {
	case valid == 0:
		res.AcceptedReason = string(gwv1.GatewayReasonListenersNotValid)
		res.AcceptedMessage = "no listener is valid"
	case valid < len(t.listeners):
		res.Accepted = true
		res.AcceptedReason = string(gwv1.GatewayReasonListenersNotValid)
		res.AcceptedMessage = fmt.Sprintf("%d of %d listeners are not valid", len(t.listeners)-valid, len(t.listeners))
	default:
		res.Accepted = true
		res.AcceptedReason = string(gwv1.GatewayReasonAccepted)
		res.AcceptedMessage = "Gateway accepted"
	}
	return res
}

// ---- listeners ----

func protocolFamily(p gwv1.ProtocolType) string {
	switch p {
	case gwv1.HTTPProtocolType:
		return "http"
	case gwv1.HTTPSProtocolType, gwv1.TLSProtocolType:
		return "tls"
	case gwv1.TCPProtocolType:
		return "tcp"
	case gwv1.UDPProtocolType:
		return "udp"
	}
	return string(p)
}

// compatibleKinds lists the route kinds each protocol can carry. gRPC
// needs HTTP/2, which Tailscale Services only negotiate on HTTPS ports.
func compatibleKinds(p gwv1.ProtocolType) []string {
	switch p {
	case gwv1.HTTPProtocolType:
		return []string{KindHTTPRoute}
	case gwv1.HTTPSProtocolType:
		return []string{KindHTTPRoute, KindGRPCRoute}
	case gwv1.TLSProtocolType:
		return []string{KindTLSRoute}
	case gwv1.TCPProtocolType:
		return []string{KindTCPRoute}
	}
	return nil
}

func (t *translator) buildListeners() {
	gw := t.in.Gateway
	portFamilies := map[gwv1.PortNumber]map[string]bool{}
	for _, l := range gw.Spec.Listeners {
		if portFamilies[l.Port] == nil {
			portFamilies[l.Port] = map[string]bool{}
		}
		portFamilies[l.Port][protocolFamily(l.Protocol)] = true
	}
	for _, l := range gw.Spec.Listeners {
		ls := &listenerState{l: l, kinds: map[string]bool{}, family: protocolFamily(l.Protocol)}
		ls.res.Name = l.Name
		var conds []metav1.Condition

		accepted := true
		compat := compatibleKinds(l.Protocol)
		switch {
		case l.Protocol == gwv1.UDPProtocolType:
			accepted = false
			conds = append(conds, cond(gwv1.ListenerConditionAccepted, false, gwv1.ListenerReasonUnsupportedProtocol,
				"UDP is not supported: Tailscale Services carry TCP only", t.gen()))
		case compat == nil:
			accepted = false
			conds = append(conds, cond(gwv1.ListenerConditionAccepted, false, gwv1.ListenerReasonUnsupportedProtocol,
				fmt.Sprintf("protocol %q is not supported", l.Protocol), t.gen()))
		case l.Protocol == gwv1.HTTPSProtocolType && l.TLS != nil && l.TLS.Mode != nil && *l.TLS.Mode == gwv1.TLSModePassthrough:
			accepted = false
			conds = append(conds, cond(gwv1.ListenerConditionAccepted, false, gwv1.ListenerReasonUnsupportedValue,
				"HTTPS listeners terminate TLS; use a TLS listener for passthrough", t.gen()))
		default:
			conds = append(conds, cond(gwv1.ListenerConditionAccepted, true, gwv1.ListenerReasonAccepted, "Listener accepted", t.gen()))
		}

		// Supported kinds.
		resolved := true
		var invalidKinds []string
		allowed := map[string]bool{}
		for _, k := range compat {
			if t.supported(k) {
				allowed[k] = true
			}
		}
		if l.AllowedRoutes != nil && len(l.AllowedRoutes.Kinds) > 0 {
			for _, rk := range l.AllowedRoutes.Kinds {
				group := gwv1.GroupName
				if rk.Group != nil {
					group = string(*rk.Group)
				}
				k := string(rk.Kind)
				if group == gwv1.GroupName && allowed[k] {
					ls.kinds[k] = true
				} else {
					invalidKinds = append(invalidKinds, k)
				}
			}
		} else {
			for k := range allowed {
				ls.kinds[k] = true
			}
		}
		for _, k := range compat { // keep a stable order
			if ls.kinds[k] {
				g := gwv1.Group(gwv1.GroupName)
				ls.res.SupportedKinds = append(ls.res.SupportedKinds, gwv1.RouteGroupKind{Group: &g, Kind: gwv1.Kind(k)})
			}
		}
		if len(invalidKinds) > 0 {
			resolved = false
			conds = append(conds, cond(gwv1.ListenerConditionResolvedRefs, false, gwv1.ListenerReasonInvalidRouteKinds,
				fmt.Sprintf("unsupported route kinds for protocol %s: %s", l.Protocol, strings.Join(invalidKinds, ", ")), t.gen()))
		} else {
			conds = append(conds, cond(gwv1.ListenerConditionResolvedRefs, true, gwv1.ListenerReasonResolvedRefs, "References resolved", t.gen()))
		}

		conflicted := len(portFamilies[l.Port]) > 1
		if conflicted {
			conds = append(conds, cond(gwv1.ListenerConditionConflicted, true, gwv1.ListenerReasonProtocolConflict,
				fmt.Sprintf("port %d is used by listeners with incompatible protocols", l.Port), t.gen()))
		} else {
			conds = append(conds, cond(gwv1.ListenerConditionConflicted, false, gwv1.ListenerReasonNoConflicts, "No conflicts", t.gen()))
		}
		ls.res.Conditions = conds
		ls.res.Valid = accepted && !conflicted && (resolved || len(ls.kinds) > 0)
		t.listeners = append(t.listeners, ls)
	}
}

// ---- routes ----

type routeItem struct {
	ref        RouteRef
	created    metav1.Time
	generation int64
	parents    []gwv1.ParentReference
	process    func(*routeCtx)
}

// routeCtx collects the outcome for one route.
type routeCtx struct {
	t    *translator
	item *routeItem
	// rule-level problems, independent of the parent
	unsupported []string
	unresolved  []string
	resolvedWhy gwv1.RouteConditionReason
	// set when no rule could be programmed for unsupported reasons
	nothingUsable bool
}

func (rc *routeCtx) unsupportedf(format string, args ...any) {
	rc.unsupported = append(rc.unsupported, fmt.Sprintf(format, args...))
}

func (rc *routeCtx) unresolvedf(reason gwv1.RouteConditionReason, format string, args ...any) {
	if rc.resolvedWhy == "" {
		rc.resolvedWhy = reason
	}
	rc.unresolved = append(rc.unresolved, fmt.Sprintf(format, args...))
}

func (t *translator) processRoutes() {
	var items []*routeItem
	for _, r := range t.in.HTTPRoutes {
		items = append(items, &routeItem{ref: RouteRef{KindHTTPRoute, r.Namespace, r.Name}, created: r.CreationTimestamp,
			generation: r.Generation, parents: r.Spec.ParentRefs, process: func(rc *routeCtx) { rc.httpRoute(r) }})
	}
	for _, r := range t.in.GRPCRoutes {
		items = append(items, &routeItem{ref: RouteRef{KindGRPCRoute, r.Namespace, r.Name}, created: r.CreationTimestamp,
			generation: r.Generation, parents: r.Spec.ParentRefs, process: func(rc *routeCtx) { rc.grpcRoute(r) }})
	}
	for _, r := range t.in.TLSRoutes {
		items = append(items, &routeItem{ref: RouteRef{KindTLSRoute, r.Namespace, r.Name}, created: r.CreationTimestamp,
			generation: r.Generation, parents: r.Spec.ParentRefs, process: func(rc *routeCtx) { rc.tlsRoute(r) }})
	}
	for _, r := range t.in.TCPRoutes {
		items = append(items, &routeItem{ref: RouteRef{KindTCPRoute, r.Namespace, r.Name}, created: r.CreationTimestamp,
			generation: r.Generation, parents: r.Spec.ParentRefs, process: func(rc *routeCtx) { rc.tcpRoute(r) }})
	}
	for _, r := range t.in.UDPRoutes {
		items = append(items, &routeItem{ref: RouteRef{KindUDPRoute, r.Namespace, r.Name}, created: r.CreationTimestamp,
			generation: r.Generation, parents: r.Spec.ParentRefs, process: func(rc *routeCtx) { rc.udpRoute() }})
	}
	// Oldest route wins conflicts, as the Gateway API spec asks.
	sort.SliceStable(items, func(i, j int) bool {
		a, b := items[i], items[j]
		if !a.created.Equal(&b.created) {
			return a.created.Before(&b.created)
		}
		if a.ref.Namespace != b.ref.Namespace {
			return a.ref.Namespace < b.ref.Namespace
		}
		if a.ref.Name != b.ref.Name {
			return a.ref.Name < b.ref.Name
		}
		return a.ref.Kind < b.ref.Kind
	})
	for _, it := range items {
		if !t.referencesGateway(it.parents, it.ref.Namespace) {
			continue
		}
		it.process(&routeCtx{t: t, item: it})
	}
}

func (t *translator) parentTargetsGateway(p gwv1.ParentReference, routeNS string) bool {
	if p.Group != nil && string(*p.Group) != gwv1.GroupName {
		return false
	}
	if p.Kind != nil && string(*p.Kind) != KindGateway {
		return false
	}
	ns := routeNS
	if p.Namespace != nil {
		ns = string(*p.Namespace)
	}
	return ns == t.in.Gateway.Namespace && string(p.Name) == t.in.Gateway.Name
}

func (t *translator) referencesGateway(parents []gwv1.ParentReference, routeNS string) bool {
	for _, p := range parents {
		if t.parentTargetsGateway(p, routeNS) {
			return true
		}
	}
	return false
}

// attachment is a listener a parentRef attached to, with the hostnames the
// route serves on it.
type attachment struct {
	ls        *listenerState
	hostnames []string // nil: no hostname constraint
}

// attach evaluates one parentRef. It returns the listeners the route
// attaches to, or a reason and message for Accepted=False.
func (rc *routeCtx) attach(p gwv1.ParentReference, routeHostnames []gwv1.Hostname, useHostnames bool) ([]attachment, gwv1.RouteConditionReason, string) {
	t := rc.t
	var candidates []*listenerState
	for _, ls := range t.listeners {
		if p.SectionName != nil && *p.SectionName != ls.l.Name {
			continue
		}
		if p.Port != nil && *p.Port != ls.l.Port {
			continue
		}
		candidates = append(candidates, ls)
	}
	if len(candidates) == 0 {
		return nil, gwv1.RouteReasonNoMatchingParent, "no listener matches the parentRef's sectionName or port"
	}
	var out []attachment
	reason, msg := gwv1.RouteReasonNotAllowedByListeners, ""
	hostnameMiss := false
	for _, ls := range candidates {
		if !ls.res.Valid {
			msg = fmt.Sprintf("listener %q is not valid", ls.l.Name)
			continue
		}
		if !ls.kinds[rc.item.ref.Kind] {
			msg = fmt.Sprintf("listener %q does not allow %s", ls.l.Name, rc.item.ref.Kind)
			if rc.item.ref.Kind == KindGRPCRoute && ls.l.Protocol == gwv1.HTTPProtocolType {
				msg += " (gRPC needs HTTP/2, which Tailscale Services only offer on HTTPS listeners)"
			}
			continue
		}
		if !t.namespaceAllowed(ls, rc.item.ref.Namespace) {
			msg = fmt.Sprintf("listener %q does not allow routes from namespace %q", ls.l.Name, rc.item.ref.Namespace)
			continue
		}
		var hosts []string
		if useHostnames {
			var ok bool
			hosts, ok = intersectHostnames(ls.l.Hostname, routeHostnames)
			if !ok {
				hostnameMiss = true
				continue
			}
		}
		out = append(out, attachment{ls: ls, hostnames: hosts})
	}
	if len(out) == 0 {
		if hostnameMiss && msg == "" {
			return nil, gwv1.RouteReasonNoMatchingListenerHostname, "no listener hostname matches the route's hostnames"
		}
		if msg == "" {
			msg = "no listener allows this route"
		}
		return nil, reason, msg
	}
	return out, "", ""
}

func (t *translator) namespaceAllowed(ls *listenerState, routeNS string) bool {
	from := gwv1.NamespacesFromSame
	var sel *metav1.LabelSelector
	if ar := ls.l.AllowedRoutes; ar != nil && ar.Namespaces != nil {
		if ar.Namespaces.From != nil {
			from = *ar.Namespaces.From
		}
		sel = ar.Namespaces.Selector
	}
	switch from {
	case gwv1.NamespacesFromAll:
		return true
	case gwv1.NamespacesFromSame:
		return routeNS == t.in.Gateway.Namespace
	case gwv1.NamespacesFromSelector:
		if sel == nil {
			return false
		}
		s, err := metav1.LabelSelectorAsSelector(sel)
		if err != nil {
			return false
		}
		return s.Matches(labels.Set(t.in.NamespaceLabels[routeNS]))
	}
	return false
}

// intersectHostnames applies the Gateway API listener/route hostname
// matching rules. It returns the effective hostnames (nil when neither side
// sets one) and whether they intersect.
func intersectHostnames(listener *gwv1.Hostname, route []gwv1.Hostname) ([]string, bool) {
	lh := ""
	if listener != nil {
		lh = strings.ToLower(string(*listener))
	}
	if len(route) == 0 {
		if lh == "" {
			return nil, true
		}
		return []string{lh}, true
	}
	var out []string
	for _, rh := range route {
		h := strings.ToLower(string(rh))
		if lh == "" {
			out = append(out, h)
			continue
		}
		if m, ok := matchHostname(lh, h); ok {
			out = append(out, m)
		}
	}
	return dedupe(out), len(out) > 0
}

// matchHostname returns the more specific of two hostnames if they match.
func matchHostname(a, b string) (string, bool) {
	if a == b {
		return a, true
	}
	aw, bw := strings.HasPrefix(a, "*."), strings.HasPrefix(b, "*.")
	switch {
	case aw && !bw:
		if strings.HasSuffix(b, a[1:]) && len(b) > len(a)-1 {
			return b, true
		}
	case bw && !aw:
		if strings.HasSuffix(a, b[1:]) && len(a) > len(b)-1 {
			return a, true
		}
	case aw && bw:
		if strings.HasSuffix(a, b[1:]) {
			return a, true
		}
		if strings.HasSuffix(b, a[1:]) {
			return b, true
		}
	}
	return "", false
}

func dedupe(in []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, s := range in {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}

// ServiceLabel maps a route hostname to a Tailscale Service name (without
// "svc:"). Tailscale Service names are single DNS labels; a hostname under
// the tailnet's MagicDNS domain is accepted and reduced to its first label.
func ServiceLabel(hostname, tailnetDomain string) (string, error) {
	h := strings.TrimSuffix(strings.ToLower(hostname), ".")
	if strings.HasPrefix(h, "*") {
		return "", fmt.Errorf("wildcard hostname %q cannot name a Tailscale Service", hostname)
	}
	label := h
	if first, rest, ok := strings.Cut(h, "."); ok {
		if !(tailnetDomain != "" && rest == strings.ToLower(tailnetDomain)) && !strings.HasSuffix(rest, ".ts.net") {
			return "", fmt.Errorf("hostname %q must be a single DNS label (for example %q), which becomes %s.<tailnet>.ts.net", hostname, first, first)
		}
		label = first
	}
	if errs := validation.IsDNS1123Label(label); len(errs) > 0 {
		return "", fmt.Errorf("hostname %q is not a valid Tailscale Service name: %s", hostname, strings.Join(errs, "; "))
	}
	return label, nil
}

// backend is a resolved backend reference.
type backend struct {
	host   string // FQDN
	port   uint16
	ns     string
	name   string
	scheme string // http or https or https+insecure (HTTP-like only)
}

// pickBackend applies weights: exactly one backend with a non-zero weight
// is supported.
func pickBackend(refs []gwv1.BackendRef) (*gwv1.BackendRef, string) {
	var nonzero []gwv1.BackendRef
	for _, r := range refs {
		if r.Weight == nil || *r.Weight > 0 {
			nonzero = append(nonzero, r)
		}
	}
	switch {
	case len(refs) == 0:
		return nil, "rule has no backendRefs"
	case len(nonzero) == 0:
		return nil, "all backendRefs have weight 0"
	case len(nonzero) > 1:
		return nil, "more than one backendRef with a non-zero weight (traffic splitting is not supported by Tailscale Services)"
	}
	return &nonzero[0], ""
}

func (rc *routeCtx) resolveBackend(ref gwv1.BackendObjectReference) (*backend, bool) {
	t := rc.t
	routeNS := rc.item.ref.Namespace
	group := ""
	if ref.Group != nil {
		group = string(*ref.Group)
	}
	kind := KindService
	if ref.Kind != nil {
		kind = string(*ref.Kind)
	}
	if group != "" || kind != KindService {
		rc.unresolvedf(gwv1.RouteReasonInvalidKind, "backend %s.%s %q is not a Service", kind, group, ref.Name)
		return nil, false
	}
	ns := routeNS
	if ref.Namespace != nil && string(*ref.Namespace) != "" {
		ns = string(*ref.Namespace)
	}
	if ns != routeNS && !t.grantAllows(rc.item.ref.Kind, routeNS, ns, string(ref.Name)) {
		rc.unresolvedf(gwv1.RouteReasonRefNotPermitted, "backend Service %s/%s is not allowed by a ReferenceGrant", ns, ref.Name)
		return nil, false
	}
	if ref.Port == nil {
		rc.unresolvedf(gwv1.RouteReasonBackendNotFound, "backend Service %s/%s needs a port", ns, ref.Name)
		return nil, false
	}
	if t.in.GetService != nil && t.in.GetService(ns, string(ref.Name)) == nil {
		rc.unresolvedf(gwv1.RouteReasonBackendNotFound, "backend Service %s/%s not found", ns, ref.Name)
		return nil, false
	}
	domain := t.in.ClusterDomain
	if domain == "" {
		domain = "cluster.local"
	}
	return &backend{
		host:   fmt.Sprintf("%s.%s.svc.%s", ref.Name, ns, domain),
		port:   uint16(*ref.Port),
		ns:     ns,
		name:   string(ref.Name),
		scheme: "http",
	}, true
}

func (t *translator) grantAllows(fromKind, fromNS, toNS, toName string) bool {
	for _, g := range t.in.ReferenceGrants {
		if g.Namespace != toNS {
			continue
		}
		fromOK := false
		for _, f := range g.Spec.From {
			if string(f.Group) == gwv1.GroupName && string(f.Kind) == fromKind && string(f.Namespace) == fromNS {
				fromOK = true
				break
			}
		}
		if !fromOK {
			continue
		}
		for _, to := range g.Spec.To {
			if string(to.Group) == "" && string(to.Kind) == KindService && (to.Name == nil || string(*to.Name) == "" || string(*to.Name) == toName) {
				return true
			}
		}
	}
	return false
}

// applyBackendTLS upgrades an HTTP-like backend to TLS when a
// BackendTLSPolicy in the backend's namespace targets it.
func (t *translator) applyBackendTLS(b *backend) {
	var match *gwv1.BackendTLSPolicy
	for _, p := range t.in.BackendTLSPolicies {
		if p.Namespace != b.ns {
			continue
		}
		for _, tr := range p.Spec.TargetRefs {
			if string(tr.Group) != "" || string(tr.Kind) != KindService || string(tr.Name) != b.name {
				continue
			}
			if tr.SectionName != nil && !t.portNameMatches(b, string(*tr.SectionName)) {
				continue
			}
			if match == nil || p.CreationTimestamp.Before(&match.CreationTimestamp) ||
				(p.CreationTimestamp.Equal(&match.CreationTimestamp) && p.Name < match.Name) {
				match = p
			}
		}
	}
	if match == nil {
		return
	}
	key := types.NamespacedName{Namespace: match.Namespace, Name: match.Name}
	v := match.Spec.Validation
	verified := len(v.CACertificateRefs) == 0 && v.WellKnownCACertificates != nil &&
		*v.WellKnownCACertificates == gwv1.WellKnownCACertificatesSystem &&
		strings.EqualFold(string(v.Hostname), b.host) && len(v.SubjectAltNames) == 0
	ps := &policyState{policy: match}
	if verified {
		b.scheme = "https"
		ps.message = "TLS to the backend is verified against the system CAs"
	} else {
		b.scheme = "https+insecure"
		ps.message = fmt.Sprintf("TLS to the backend is used, but the certificate is not verified: "+
			"Tailscale serve can only verify against system CAs with the hostname %s", b.host)
	}
	t.policies[key] = ps
}

func (t *translator) portNameMatches(b *backend, name string) bool {
	if t.in.GetService == nil {
		return true
	}
	svc := t.in.GetService(b.ns, b.name)
	if svc == nil {
		return false
	}
	for _, p := range svc.Spec.Ports {
		if p.Name == name && uint16(p.Port) == b.port {
			return true
		}
	}
	return false
}

func (t *translator) policyResults() []PolicyResult {
	var out []PolicyResult
	for key, ps := range t.policies {
		out = append(out, PolicyResult{Ref: key, Conditions: []metav1.Condition{
			cond(gwv1.PolicyConditionAccepted, true, gwv1.PolicyReasonAccepted, ps.message, ps.policy.Generation),
		}})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Ref.String() < out[j].Ref.String() })
	return out
}

// ---- HTTP and gRPC ----

type httpMount struct {
	path    string
	backend *backend
}

func normalizePrefix(p string) string {
	if p == "" || p == "/" {
		return "/"
	}
	return strings.TrimRight(p, "/")
}

func (rc *routeCtx) httpRoute(r *gwv1.HTTPRoute) {
	var mounts []httpMount
	usableRules := 0
	for i, rule := range r.Spec.Rules {
		name := ruleName(i, rule.Name)
		if len(rule.Filters) > 0 {
			rc.unsupportedf("%s: filters are not supported", name)
			continue
		}
		if rule.Timeouts != nil || rule.Retry != nil || rule.SessionPersistence != nil {
			rc.unsupportedf("%s: timeouts, retries and session persistence are not supported", name)
			continue
		}
		var paths []string
		if len(rule.Matches) == 0 {
			paths = []string{"/"}
		}
		for j, m := range rule.Matches {
			if len(m.Headers) > 0 || len(m.QueryParams) > 0 || m.Method != nil {
				rc.unsupportedf("%s match %d: header, query and method matches are not supported", name, j)
				continue
			}
			typ, val := gwv1.PathMatchPathPrefix, "/"
			if m.Path != nil {
				if m.Path.Type != nil {
					typ = *m.Path.Type
				}
				if m.Path.Value != nil {
					val = *m.Path.Value
				}
			}
			if typ != gwv1.PathMatchPathPrefix {
				rc.unsupportedf("%s match %d: path match type %s is not supported (only PathPrefix)", name, j, typ)
				continue
			}
			paths = append(paths, normalizePrefix(val))
		}
		if len(paths) == 0 {
			continue
		}
		var refs []gwv1.BackendRef
		bad := false
		for _, br := range rule.BackendRefs {
			if len(br.Filters) > 0 {
				rc.unsupportedf("%s: backendRef filters are not supported", name)
				bad = true
				break
			}
			refs = append(refs, br.BackendRef)
		}
		if bad {
			continue
		}
		br, why := pickBackend(refs)
		if br == nil {
			rc.unsupportedf("%s: %s", name, why)
			continue
		}
		usableRules++
		b, ok := rc.resolveBackend(br.BackendObjectReference)
		if !ok {
			continue
		}
		rc.t.applyBackendTLS(b)
		for _, p := range paths {
			mounts = append(mounts, httpMount{path: p, backend: b})
		}
	}
	rc.nothingUsable = len(r.Spec.Rules) > 0 && usableRules == 0
	rc.finishHTTPLike(r.Spec.Hostnames, mounts)
}

func (rc *routeCtx) grpcRoute(r *gwv1.GRPCRoute) {
	var mounts []httpMount
	usableRules := 0
	for i, rule := range r.Spec.Rules {
		name := ruleName(i, rule.Name)
		if len(rule.Filters) > 0 {
			rc.unsupportedf("%s: filters are not supported", name)
			continue
		}
		if rule.SessionPersistence != nil {
			rc.unsupportedf("%s: session persistence is not supported", name)
			continue
		}
		var paths []string
		if len(rule.Matches) == 0 {
			paths = []string{"/"}
		}
		for j, m := range rule.Matches {
			if len(m.Headers) > 0 {
				rc.unsupportedf("%s match %d: header matches are not supported", name, j)
				continue
			}
			if m.Method == nil {
				paths = append(paths, "/")
				continue
			}
			if m.Method.Type != nil && *m.Method.Type != gwv1.GRPCMethodMatchExact {
				rc.unsupportedf("%s match %d: method match type %s is not supported (only Exact)", name, j, *m.Method.Type)
				continue
			}
			svc, method := deref(m.Method.Service), deref(m.Method.Method)
			switch {
			case svc != "" && method != "":
				paths = append(paths, "/"+svc+"/"+method)
			case svc != "":
				paths = append(paths, "/"+svc)
			default:
				rc.unsupportedf("%s match %d: a method match without a service is not supported", name, j)
			}
		}
		if len(paths) == 0 {
			continue
		}
		var refs []gwv1.BackendRef
		bad := false
		for _, br := range rule.BackendRefs {
			if len(br.Filters) > 0 {
				rc.unsupportedf("%s: backendRef filters are not supported", name)
				bad = true
				break
			}
			refs = append(refs, br.BackendRef)
		}
		if bad {
			continue
		}
		br, why := pickBackend(refs)
		if br == nil {
			rc.unsupportedf("%s: %s", name, why)
			continue
		}
		usableRules++
		b, ok := rc.resolveBackend(br.BackendObjectReference)
		if !ok {
			continue
		}
		rc.t.applyBackendTLS(b)
		for _, p := range paths {
			mounts = append(mounts, httpMount{path: p, backend: b})
		}
	}
	rc.nothingUsable = len(r.Spec.Rules) > 0 && usableRules == 0
	rc.finishHTTPLike(r.Spec.Hostnames, mounts)
}

func (rc *routeCtx) finishHTTPLike(hostnames []gwv1.Hostname, mounts []httpMount) {
	t := rc.t
	rr := RouteResult{Ref: rc.item.ref}
	for _, p := range rc.item.parents {
		if !t.parentTargetsGateway(p, rc.item.ref.Namespace) {
			continue
		}
		atts, reason, msg := rc.attach(p, hostnames, true)
		if atts == nil {
			rr.Parents = append(rr.Parents, rc.parentResult(p, false, reason, msg, nil))
			continue
		}
		var labelErr string
		var conflicts []string
		placed, attached := 0, 0
		for _, a := range atts {
			hosts := a.hostnames
			var labels []string
			if len(hosts) == 0 {
				l, err := ServiceLabel(t.in.Gateway.Name, t.in.TailnetDomain)
				if err != nil {
					labelErr = fmt.Sprintf("route has no hostname and the Gateway name cannot be used as a Tailscale Service name: %v", err)
					continue
				}
				labels = []string{l}
			}
			for _, h := range hosts {
				l, err := ServiceLabel(h, t.in.TailnetDomain)
				if err != nil {
					labelErr = err.Error()
					continue
				}
				labels = append(labels, l)
			}
			if len(labels) == 0 {
				continue
			}
			attached++
			a.ls.res.AttachedRoutes++
			mode := "http"
			if a.ls.l.Protocol == gwv1.HTTPSProtocolType {
				mode = "https"
			}
			for _, label := range dedupe(labels) {
				for _, m := range mounts {
					if c := t.addMount(label, uint16(a.ls.l.Port), mode, m, rc.item.ref); c != "" {
						conflicts = append(conflicts, c)
					} else {
						placed++
					}
				}
			}
		}
		if attached == 0 {
			rr.Parents = append(rr.Parents, rc.parentResult(p, false, gwv1.RouteReasonUnsupportedValue, labelErr, nil))
			continue
		}
		extra := conflicts
		if labelErr != "" {
			extra = append(extra, labelErr)
		}
		if len(mounts) > 0 && placed == 0 && len(conflicts) > 0 {
			rr.Parents = append(rr.Parents, rc.parentResult(p, false, ReasonConflicted, strings.Join(dedupe(conflicts), "; "), nil))
			continue
		}
		rr.Parents = append(rr.Parents, rc.parentResult(p, true, "", "", dedupe(extra)))
	}
	t.routes = append(t.routes, rr)
}

func (t *translator) svc(label string) *svcState {
	s := t.services[label]
	if s == nil {
		s = &svcState{label: label, ports: map[uint16]*portState{}}
		t.services[label] = s
	}
	return s
}

// addMount places an HTTP mount; it returns a conflict message or "".
func (t *translator) addMount(label string, port uint16, mode string, m httpMount, owner RouteRef) string {
	s := t.svc(label)
	ps := s.ports[port]
	if ps == nil {
		ps = &portState{mode: mode, owner: owner, mounts: map[string]mount{}}
		s.ports[port] = ps
	}
	if ps.mode != mode {
		return fmt.Sprintf("svc:%s port %d is already used as %s by %s", label, port, ps.mode, ps.owner)
	}
	proxy := fmt.Sprintf("%s://%s:%d%s", m.backend.scheme, m.backend.host, m.backend.port, m.path)
	if existing, ok := ps.mounts[m.path]; ok {
		if existing.owner != owner {
			return fmt.Sprintf("path %s on svc:%s port %d is already used by %s", m.path, label, port, existing.owner)
		}
		return "" // same route, earlier rule wins
	}
	ps.mounts[m.path] = mount{proxy: proxy, owner: owner}
	ps.order = append(ps.order, m.path)
	return ""
}

// ---- TCP and TLS ----

func (rc *routeCtx) tcpRoute(r *gwv1.TCPRoute) {
	b := rc.singleBackend(tcpRules(r.Spec.Rules))
	label := r.Name
	if v := r.Annotations[config.AnnotationTailscaleService]; v != "" {
		label = v
	}
	rc.finishStream(nil, false, label, b)
}

func (rc *routeCtx) tlsRoute(r *gwv1.TLSRoute) {
	b := rc.singleBackend(tlsRules(r.Spec.Rules))
	label := r.Name
	if v := r.Annotations[config.AnnotationTailscaleService]; v != "" {
		label = v
	}
	rc.finishStream(r.Spec.Hostnames, true, label, b)
}

func tcpRules(rules []gwv1.TCPRouteRule) [][]gwv1.BackendRef {
	out := make([][]gwv1.BackendRef, len(rules))
	for i, r := range rules {
		out[i] = r.BackendRefs
	}
	return out
}

func tlsRules(rules []gwv1.TLSRouteRule) [][]gwv1.BackendRef {
	out := make([][]gwv1.BackendRef, len(rules))
	for i, r := range rules {
		out[i] = r.BackendRefs
	}
	return out
}

func (rc *routeCtx) singleBackend(rules [][]gwv1.BackendRef) *backend {
	if len(rules) == 0 {
		rc.unsupportedf("route has no rules")
		rc.nothingUsable = true
		return nil
	}
	if len(rules) > 1 {
		rc.unsupportedf("only the first rule is used")
	}
	br, why := pickBackend(rules[0])
	if br == nil {
		rc.unsupportedf("rule 0: %s", why)
		rc.nothingUsable = true
		return nil
	}
	b, ok := rc.resolveBackend(br.BackendObjectReference)
	if !ok {
		return nil
	}
	return b
}

func (rc *routeCtx) finishStream(hostnames []gwv1.Hostname, useHostnames bool, defaultLabel string, b *backend) {
	t := rc.t
	rr := RouteResult{Ref: rc.item.ref}
	for _, p := range rc.item.parents {
		if !t.parentTargetsGateway(p, rc.item.ref.Namespace) {
			continue
		}
		atts, reason, msg := rc.attach(p, hostnames, useHostnames)
		if atts == nil {
			rr.Parents = append(rr.Parents, rc.parentResult(p, false, reason, msg, nil))
			continue
		}
		var problems, conflicts []string
		attached, placed := 0, 0
		for _, a := range atts {
			var labels []string
			if len(a.hostnames) == 0 {
				labels = []string{defaultLabel}
			}
			for _, h := range a.hostnames {
				l, err := ServiceLabel(h, t.in.TailnetDomain)
				if err != nil {
					problems = append(problems, err.Error())
					continue
				}
				labels = append(labels, l)
			}
			var valid []string
			for _, l := range labels {
				if errs := validation.IsDNS1123Label(l); len(errs) > 0 {
					problems = append(problems, fmt.Sprintf("%q is not a valid Tailscale Service name: %s", l, strings.Join(errs, "; ")))
					continue
				}
				valid = append(valid, l)
			}
			if len(valid) == 0 {
				continue
			}
			attached++
			a.ls.res.AttachedRoutes++
			if b == nil {
				continue
			}
			mode := "tcp"
			if a.ls.l.Protocol == gwv1.TLSProtocolType && (a.ls.l.TLS == nil || a.ls.l.TLS.Mode == nil || *a.ls.l.TLS.Mode == gwv1.TLSModeTerminate) {
				mode = "tls"
			}
			for _, l := range dedupe(valid) {
				if c := t.addStream(l, uint16(a.ls.l.Port), mode, hostPort(b), rc.item.ref); c != "" {
					conflicts = append(conflicts, c)
				} else {
					placed++
				}
			}
		}
		switch {
		case attached == 0:
			rr.Parents = append(rr.Parents, rc.parentResult(p, false, gwv1.RouteReasonUnsupportedValue, strings.Join(problems, "; "), nil))
		case b != nil && placed == 0 && len(conflicts) > 0:
			rr.Parents = append(rr.Parents, rc.parentResult(p, false, ReasonConflicted, strings.Join(dedupe(conflicts), "; "), nil))
		default:
			rr.Parents = append(rr.Parents, rc.parentResult(p, true, "", "", dedupe(append(conflicts, problems...))))
		}
	}
	t.routes = append(t.routes, rr)
}

func hostPort(b *backend) string { return b.host + ":" + strconv.Itoa(int(b.port)) }

func (t *translator) addStream(label string, port uint16, mode, target string, owner RouteRef) string {
	s := t.svc(label)
	if ps := s.ports[port]; ps != nil {
		if ps.owner == owner && ps.mode == mode && ps.target == target {
			return ""
		}
		return fmt.Sprintf("svc:%s port %d is already used by %s", label, port, ps.owner)
	}
	s.ports[port] = &portState{mode: mode, owner: owner, target: target}
	return ""
}

func (rc *routeCtx) udpRoute() {
	t := rc.t
	rr := RouteResult{Ref: rc.item.ref}
	for _, p := range rc.item.parents {
		if !t.parentTargetsGateway(p, rc.item.ref.Namespace) {
			continue
		}
		rr.Parents = append(rr.Parents, ParentResult{ParentRef: p, Conditions: []metav1.Condition{
			cond(gwv1.RouteConditionAccepted, false, gwv1.RouteReasonUnsupportedValue,
				"UDPRoute is not supported: Tailscale Services carry TCP only", rc.item.generation),
			cond(gwv1.RouteConditionResolvedRefs, true, gwv1.RouteReasonResolvedRefs, "", rc.item.generation),
		}})
	}
	t.routes = append(t.routes, rr)
}

// parentResult builds the conditions for one parentRef.
func (rc *routeCtx) parentResult(p gwv1.ParentReference, attached bool, reason gwv1.RouteConditionReason, msg string, extra []string) ParentResult {
	g := rc.item.generation
	var conds []metav1.Condition
	switch {
	case !attached:
		conds = append(conds, cond(gwv1.RouteConditionAccepted, false, reason, msg, g))
	case rc.nothingUsable:
		conds = append(conds, cond(gwv1.RouteConditionAccepted, false, gwv1.RouteReasonUnsupportedValue,
			"no rule can be programmed: "+strings.Join(rc.unsupported, "; "), g))
	default:
		conds = append(conds, cond(gwv1.RouteConditionAccepted, true, gwv1.RouteReasonAccepted, "Route accepted", g))
		problems := append(append([]string{}, rc.unsupported...), extra...)
		if len(problems) > 0 {
			conds = append(conds, cond(gwv1.RouteConditionPartiallyInvalid, true, gwv1.RouteReasonUnsupportedValue,
				strings.Join(problems, "; "), g))
		}
	}
	if len(rc.unresolved) > 0 {
		conds = append(conds, cond(gwv1.RouteConditionResolvedRefs, false, rc.resolvedWhy, strings.Join(rc.unresolved, "; "), g))
	} else {
		conds = append(conds, cond(gwv1.RouteConditionResolvedRefs, true, gwv1.RouteReasonResolvedRefs, "References resolved", g))
	}
	return ParentResult{ParentRef: p, Conditions: conds}
}

// ---- rendering ----

func (t *translator) renderConfig() *serveconf.Config {
	cfg := &serveconf.Config{Services: map[string]*serveconf.Service{}}
	domain := t.in.TailnetDomain
	for label, s := range t.services {
		out := &serveconf.Service{}
		for port, ps := range s.ports {
			switch ps.mode {
			case "http", "https":
				if len(ps.mounts) == 0 {
					continue
				}
				if domain == "" {
					t.pending = true
					continue
				}
				if out.TCP == nil {
					out.TCP = map[uint16]*serveconf.TCPPortHandler{}
					out.Web = map[string]*serveconf.WebServer{}
				}
				out.TCP[port] = &serveconf.TCPPortHandler{HTTP: ps.mode == "http", HTTPS: ps.mode == "https"}
				ws := &serveconf.WebServer{Handlers: map[string]*serveconf.HTTPHandler{}}
				for path, m := range ps.mounts {
					ws.Handlers[path] = &serveconf.HTTPHandler{Proxy: m.proxy}
				}
				out.Web[serveconf.WebKey(label, domain, port)] = ws
			case "tcp":
				if out.TCP == nil {
					out.TCP = map[uint16]*serveconf.TCPPortHandler{}
				}
				out.TCP[port] = &serveconf.TCPPortHandler{TCPForward: ps.target}
			case "tls":
				if domain == "" {
					t.pending = true
					continue
				}
				if out.TCP == nil {
					out.TCP = map[uint16]*serveconf.TCPPortHandler{}
				}
				out.TCP[port] = &serveconf.TCPPortHandler{TCPForward: ps.target, TerminateTLS: label + "." + domain}
			}
		}
		if len(out.TCP) > 0 {
			cfg.Services[serveconf.ServiceName(label)] = out
		}
	}
	return cfg
}

// ---- helpers ----

func cond[T ~string, R ~string](typ T, status bool, reason R, msg string, gen int64) metav1.Condition {
	s := metav1.ConditionFalse
	if status {
		s = metav1.ConditionTrue
	}
	if len(msg) > 32000 {
		msg = msg[:32000]
	}
	return metav1.Condition{Type: string(typ), Status: s, Reason: string(reason), Message: msg, ObservedGeneration: gen}
}

func ruleName(i int, n *gwv1.SectionName) string {
	if n != nil && *n != "" {
		return fmt.Sprintf("rule %q", *n)
	}
	return fmt.Sprintf("rule %d", i)
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
