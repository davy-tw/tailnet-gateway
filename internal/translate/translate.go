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
	"k8s.io/utils/ptr"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"

	"go.pkgs.davy.tw/tailnet-gateway/internal/config"
	"go.pkgs.davy.tw/tailnet-gateway/internal/serveconf"
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
	// KindListenerSet is a set of listeners attached to a Gateway, which
	// routes can also name as their parent.
	KindListenerSet = "ListenerSet"
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

	// ListenerSets are the ListenerSets whose parentRef names the Gateway.
	// Whether the Gateway allows them is decided here.
	ListenerSets []*gwv1.ListenerSet

	// NamespaceLabels maps namespace name to its labels, for listener
	// allowedRoutes selectors and the Gateway's allowedListeners selector.
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
	// ClaimedServices maps a Tailscale Service name (without "svc:") to
	// the older Gateway ("namespace/name") that already serves it. Names
	// are tailnet-wide, so a Gateway never serves a name another Gateway
	// has claimed: their hosts would share its traffic.
	ClaimedServices map[string]string
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
	// InvalidReason is the reason for Programmed=False on a listener that
	// is not valid.
	InvalidReason string
}

// ListenerSetResult is the status of one ListenerSet whose parentRef names
// the Gateway.
type ListenerSetResult struct {
	Ref        types.NamespacedName
	Generation int64
	// Accepted ListenerSets are attached to the Gateway: they are allowed
	// and have at least one valid listener.
	Accepted bool
	// Reason and Message describe the Accepted condition.
	Reason  string
	Message string
	// Listeners is empty when the Gateway does not allow the ListenerSet.
	Listeners []ListenerResult
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
	// ListenerSets is in listener precedence order (oldest first).
	ListenerSets []ListenerSetResult
	// AttachedListenerSets counts the accepted ListenerSets.
	AttachedListenerSets int32
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
		sets:     map[types.NamespacedName]*setState{},

		labelOwner:    map[string]int{},
		labelRoutes:   map[string]map[RouteRef]bool{},
		parentResults: map[RouteRef]map[int]ParentResult{},
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
	// sets holds every ListenerSet naming the Gateway; setOrder is their
	// precedence order.
	sets     map[types.NamespacedName]*setState
	setOrder []*setState
	// labelOwner maps a Tailscale Service label to the rank of the
	// listener owner it belongs to: 0 for the Gateway, then the allowed
	// ListenerSets in precedence order. A label is shared on every port,
	// so one owner holds it on all of them.
	labelOwner map[string]int
	// labelRoutes records the routes placed on each label.
	labelRoutes map[string]map[RouteRef]bool
	// earlier holds the accepted listeners that later ListenerSet
	// listeners must not conflict with.
	earlier []*listenerState
	// parentResults collects route results per parentRef index while
	// routes are processed owner by owner.
	parentResults map[RouteRef]map[int]ParentResult
}

type listenerState struct {
	l      gwv1.Listener
	res    ListenerResult
	kinds  map[string]bool
	family string
	// set is the ListenerSet that defines the listener; nil for the
	// Gateway's own listeners.
	set *setState
	// ns is the namespace of the listener's owner, which allowedRoutes
	// "Same" refers to.
	ns string
	// accepted is false for listeners that can never carry traffic
	// (unsupported protocol or TLS mode).
	accepted   bool
	conflicted bool
}

// rank is the precedence of the listener's owner: 0 for the Gateway.
func (ls *listenerState) rank() int {
	if ls.set == nil {
		return 0
	}
	return ls.set.rank
}

type setState struct {
	ls        *gwv1.ListenerSet
	allowed   bool
	accepted  bool
	listeners []*listenerState
	// rank orders allowed ListenerSets after the Gateway (from 1).
	rank int
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
	policy   *gwv1.BackendTLSPolicy
	accepted bool
	message  string
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
	// Owners go in precedence order: the Gateway's listeners and routes,
	// then each allowed ListenerSet's listeners and routes. Everything an
	// earlier owner uses (ports, Tailscale Service names) is settled before
	// a later owner's listeners are checked against it.
	t.prepareListenerSets()
	routed := t.collectRoutes()
	t.processRank(routed, 0)
	for _, ss := range t.setOrder {
		if ss.allowed {
			t.buildSetListeners(ss)
			t.processRank(routed, ss.rank)
		}
	}
	t.assembleRoutes(routed)

	res := &Result{
		Config:     t.renderConfig(),
		Routes:     t.routes,
		WebPending: t.pending,
	}
	for _, ls := range t.listeners {
		if ls.set == nil {
			res.Listeners = append(res.Listeners, ls.res)
		}
	}
	for label := range t.services {
		res.Services = append(res.Services, label)
	}
	sort.Strings(res.Services)
	res.Policies = t.policyResults()

	valid, own := 0, 0
	for _, ls := range t.listeners {
		if ls.set != nil {
			continue
		}
		own++
		if ls.res.Valid {
			valid++
		}
	}
	switch {
	case own == 0:
		// Every listener comes from ListenerSets.
		res.Accepted = true
		res.AcceptedReason = string(gwv1.GatewayReasonAccepted)
		res.AcceptedMessage = "Gateway accepted"
	case valid == 0:
		res.AcceptedReason = string(gwv1.GatewayReasonListenersNotValid)
		res.AcceptedMessage = "no listener is valid"
	case valid < own:
		res.Accepted = true
		res.AcceptedReason = string(gwv1.GatewayReasonListenersNotValid)
		res.AcceptedMessage = fmt.Sprintf("%d of %d listeners are not valid", own-valid, own)
	default:
		res.Accepted = true
		res.AcceptedReason = string(gwv1.GatewayReasonAccepted)
		res.AcceptedMessage = "Gateway accepted"
	}
	res.ListenerSets, res.AttachedListenerSets = t.listenerSetResults(res.Accepted)
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
	// The Gateway's default Service (routes without hostnames on listeners
	// without one) and its listener hostnames belong to the Gateway.
	if l, err := ServiceLabel(gw.Name, t.in.TailnetDomain); err == nil {
		t.labelOwner[l] = 0
	}
	for _, l := range gw.Spec.Listeners {
		if label := t.listenerLabel(l.Hostname); label != "" {
			t.labelOwner[label] = 0
		}
	}
	for _, l := range gw.Spec.Listeners {
		ls := t.newListener(l, gw.Namespace, nil, t.gen())
		// Listeners of the Gateway itself that conflict are all left out:
		// the Gateway API does not let one of them win.
		if len(portFamilies[l.Port]) > 1 {
			ls.conflicted = true
			ls.res.Valid = false
			ls.res.InvalidReason = string(gwv1.ListenerReasonInvalid)
			replaceCond(&ls.res.Conditions, cond(gwv1.ListenerConditionConflicted, true, gwv1.ListenerReasonProtocolConflict,
				fmt.Sprintf("port %d is used by listeners with incompatible protocols", l.Port), t.gen()))
		}
		t.listeners = append(t.listeners, ls)
	}
}

// newListener evaluates a listener on its own: protocol, TLS mode and
// route kinds. Conflicted starts out false.
func (t *translator) newListener(l gwv1.Listener, ns string, set *setState, gen int64) *listenerState {
	ls := &listenerState{l: l, kinds: map[string]bool{}, family: protocolFamily(l.Protocol), set: set, ns: ns, accepted: true}
	ls.res.Name = l.Name
	var conds []metav1.Condition

	compat := compatibleKinds(l.Protocol)
	switch {
	case l.Protocol == gwv1.UDPProtocolType:
		ls.accepted = false
		conds = append(conds, cond(gwv1.ListenerConditionAccepted, false, gwv1.ListenerReasonUnsupportedProtocol,
			"UDP is not supported: Tailscale Services carry TCP only", gen))
	case compat == nil:
		ls.accepted = false
		conds = append(conds, cond(gwv1.ListenerConditionAccepted, false, gwv1.ListenerReasonUnsupportedProtocol,
			fmt.Sprintf("protocol %q is not supported", l.Protocol), gen))
	case l.Protocol == gwv1.HTTPSProtocolType && l.TLS != nil && l.TLS.Mode != nil && *l.TLS.Mode == gwv1.TLSModePassthrough:
		ls.accepted = false
		conds = append(conds, cond(gwv1.ListenerConditionAccepted, false, gwv1.ListenerReasonUnsupportedValue,
			"HTTPS listeners terminate TLS; use a TLS listener for passthrough", gen))
	default:
		conds = append(conds, cond(gwv1.ListenerConditionAccepted, true, gwv1.ListenerReasonAccepted, "Listener accepted", gen))
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
			fmt.Sprintf("unsupported route kinds for protocol %s: %s", l.Protocol, strings.Join(invalidKinds, ", ")), gen))
	} else {
		conds = append(conds, cond(gwv1.ListenerConditionResolvedRefs, true, gwv1.ListenerReasonResolvedRefs, "References resolved", gen))
	}
	conds = append(conds, cond(gwv1.ListenerConditionConflicted, false, gwv1.ListenerReasonNoConflicts, "No conflicts", gen))
	ls.res.Conditions = conds
	ls.res.Valid = ls.accepted && (resolved || len(ls.kinds) > 0)
	if !ls.res.Valid {
		ls.res.InvalidReason = string(gwv1.ListenerReasonInvalid)
	}
	return ls
}

// ---- ListenerSets ----

// listenerSetParent reports whether a ListenerSet's parentRef names the
// Gateway.
func listenerSetParent(ls *gwv1.ListenerSet, gw *gwv1.Gateway) bool {
	p := ls.Spec.ParentRef
	if p.Group != nil && string(*p.Group) != gwv1.GroupName {
		return false
	}
	if p.Kind != nil && string(*p.Kind) != KindGateway {
		return false
	}
	ns := ls.Namespace
	if p.Namespace != nil && *p.Namespace != "" {
		ns = string(*p.Namespace)
	}
	return ns == gw.Namespace && string(p.Name) == gw.Name
}

// ListenerSetTargets reports whether a ListenerSet's parentRef names the
// Gateway namespace/name.
func ListenerSetTargets(ls *gwv1.ListenerSet, ns, name string) bool {
	return listenerSetParent(ls, &gwv1.Gateway{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name}})
}

// listenerSetAllowed applies the Gateway's allowedListeners. Without it,
// no ListenerSet may attach.
func (t *translator) listenerSetAllowed(ls *gwv1.ListenerSet) bool {
	gw := t.in.Gateway
	from := gwv1.NamespacesFromNone
	var sel *metav1.LabelSelector
	if al := gw.Spec.AllowedListeners; al != nil && al.Namespaces != nil {
		if al.Namespaces.From != nil {
			from = *al.Namespaces.From
		}
		sel = al.Namespaces.Selector
	}
	switch from {
	case gwv1.NamespacesFromAll:
		return true
	case gwv1.NamespacesFromSame:
		return ls.Namespace == gw.Namespace
	case gwv1.NamespacesFromSelector:
		if sel == nil {
			return false
		}
		s, err := metav1.LabelSelectorAsSelector(sel)
		if err != nil {
			return false
		}
		return s.Matches(labels.Set(t.in.NamespaceLabels[ls.Namespace]))
	}
	return false
}

func entryListener(e gwv1.ListenerEntry) gwv1.Listener {
	return gwv1.Listener{Name: e.Name, Hostname: e.Hostname, Port: e.Port, Protocol: e.Protocol, TLS: e.TLS, AllowedRoutes: e.AllowedRoutes}
}

// listenerLabel returns the Tailscale Service label a listener hostname
// maps to, or "" when it has no hostname or the hostname cannot name one.
func (t *translator) listenerLabel(h *gwv1.Hostname) string {
	if h == nil || *h == "" {
		return ""
	}
	l, err := ServiceLabel(string(*h), t.in.TailnetDomain)
	if err != nil {
		return ""
	}
	return l
}

// hostnameKey compares listener hostnames by the Tailscale Service they
// name ("admin" and "admin.<tailnet>.ts.net" are the same), or by the
// normalized hostname when they name none.
func (t *translator) hostnameKey(h *gwv1.Hostname) string {
	if l := t.listenerLabel(h); l != "" {
		return "svc:" + l
	}
	if h == nil {
		return ""
	}
	return strings.TrimSuffix(strings.ToLower(string(*h)), ".")
}

// conflictWith returns the conflict reason and message when l cannot be
// served next to an earlier listener: another protocol family on the same
// port, or the same protocol family and hostname on the same port.
func (t *translator) conflictWith(l gwv1.Listener, earlier []*listenerState) (gwv1.ListenerConditionReason, string) {
	for _, e := range earlier {
		if e.l.Port != l.Port {
			continue
		}
		if e.family != protocolFamily(l.Protocol) {
			return gwv1.ListenerReasonProtocolConflict, fmt.Sprintf("port %d is already used with protocol %s by %s", l.Port, e.l.Protocol, e.describe())
		}
		if t.hostnameKey(e.l.Hostname) == t.hostnameKey(l.Hostname) {
			return gwv1.ListenerReasonHostnameConflict, fmt.Sprintf("port %d and hostname %q are already used by %s", l.Port, ptr.Deref(l.Hostname, ""), e.describe())
		}
	}
	return "", ""
}

// ownerName describes the listener owner of a rank.
func (t *translator) ownerName(rank int) string {
	if rank == 0 {
		return fmt.Sprintf("Gateway %s/%s", t.in.Gateway.Namespace, t.in.Gateway.Name)
	}
	for _, ss := range t.setOrder {
		if ss.allowed && ss.rank == rank {
			return fmt.Sprintf("ListenerSet %s/%s", ss.ls.Namespace, ss.ls.Name)
		}
	}
	return "another listener owner"
}

func (ls *listenerState) describe() string {
	if ls.set == nil {
		return fmt.Sprintf("Gateway listener %q", ls.l.Name)
	}
	return fmt.Sprintf("ListenerSet %s/%s listener %q", ls.set.ls.Namespace, ls.set.ls.Name, ls.l.Name)
}

// buildListenerSets merges the listeners of the allowed ListenerSets after
// the Gateway's own, in the Gateway API precedence order: the Gateway,
// then ListenerSets by creation time, then by namespace/name. A listener
// that conflicts with an earlier one is not accepted; the earlier one
// keeps serving.
func (t *translator) prepareListenerSets() {
	var sets []*gwv1.ListenerSet
	for _, ls := range t.in.ListenerSets {
		if listenerSetParent(ls, t.in.Gateway) {
			sets = append(sets, ls)
		}
	}
	sort.SliceStable(sets, func(i, j int) bool {
		a, b := sets[i], sets[j]
		if !a.CreationTimestamp.Equal(&b.CreationTimestamp) {
			return a.CreationTimestamp.Before(&b.CreationTimestamp)
		}
		return a.Namespace+"/"+a.Name < b.Namespace+"/"+b.Name
	})
	// The Gateway's listeners keep their ports even when they conflict
	// among themselves: those ports were declared by the Gateway's owner.
	for _, ls := range t.listeners {
		if ls.accepted {
			t.earlier = append(t.earlier, ls)
		}
	}
	rank := 0
	for _, ls := range sets {
		ss := &setState{ls: ls, allowed: t.listenerSetAllowed(ls)}
		key := types.NamespacedName{Namespace: ls.Namespace, Name: ls.Name}
		t.sets[key] = ss
		t.setOrder = append(t.setOrder, ss)
		if ss.allowed {
			rank++
			ss.rank = rank
		}
	}
}

// buildSetListeners adds an allowed ListenerSet's listeners. A listener
// that conflicts with an earlier one (same port), or whose hostname names
// a Tailscale Service an earlier owner uses (any port), is not accepted.
func (t *translator) buildSetListeners(ss *setState) {
	ls, rank := ss.ls, ss.rank
	for _, e := range ls.Spec.Listeners {
		l := entryListener(e)
		st := t.newListener(l, ls.Namespace, ss, ls.Generation)
		if st.accepted {
			reason, msg := t.conflictWith(l, t.earlier)
			label := t.listenerLabel(l.Hostname)
			if owner, ok := t.labelOwner[label]; reason == "" && label != "" && ok && owner != rank {
				// A Tailscale Service is shared on every port: a
				// hostname that names another owner's Service is
				// taken, whatever the port or protocol.
				reason = gwv1.ListenerReasonHostnameConflict
				msg = fmt.Sprintf("hostname %q names svc:%s, which belongs to %s", ptr.Deref(l.Hostname, ""), label, t.ownerName(owner))
			}
			if reason == "" && label != "" {
				t.labelOwner[label] = rank
			}
			if reason != "" {
				st.conflicted = true
				st.res.Valid = false
				st.res.InvalidReason = string(reason)
				replaceCond(&st.res.Conditions, cond(gwv1.ListenerConditionAccepted, false, reason, msg, ls.Generation))
				replaceCond(&st.res.Conditions, cond(gwv1.ListenerConditionConflicted, true, reason, msg, ls.Generation))
			} else {
				t.earlier = append(t.earlier, st)
			}
		}
		ss.listeners = append(ss.listeners, st)
		if st.res.Valid {
			ss.accepted = true
		}
		t.listeners = append(t.listeners, st)
	}
}

// listenerSetResults builds the ListenerSet statuses once routes are
// attached. A ListenerSet of a Gateway that is not accepted is not
// accepted either.
func (t *translator) listenerSetResults(gatewayAccepted bool) ([]ListenerSetResult, int32) {
	var out []ListenerSetResult
	var attached int32
	for _, ss := range t.setOrder {
		r := ListenerSetResult{Ref: types.NamespacedName{Namespace: ss.ls.Namespace, Name: ss.ls.Name}, Generation: ss.ls.Generation}
		for _, l := range ss.listeners {
			r.Listeners = append(r.Listeners, l.res)
		}
		valid := 0
		for _, l := range ss.listeners {
			if l.res.Valid {
				valid++
			}
		}
		switch {
		case !ss.allowed:
			r.Reason = string(gwv1.ListenerSetReasonNotAllowed)
			r.Message = fmt.Sprintf("Gateway %s/%s does not allow ListenerSets from namespace %q (spec.allowedListeners)",
				t.in.Gateway.Namespace, t.in.Gateway.Name, ss.ls.Namespace)
		case !gatewayAccepted:
			r.Reason = string(gwv1.ListenerSetReasonParentNotAccepted)
			r.Message = fmt.Sprintf("Gateway %s/%s is not accepted", t.in.Gateway.Namespace, t.in.Gateway.Name)
		case valid == 0:
			r.Reason = string(gwv1.ListenerSetReasonListenersNotValid)
			r.Message = "no listener is valid"
		case valid < len(ss.listeners):
			r.Accepted = true
			r.Reason = string(gwv1.ListenerSetReasonListenersNotValid)
			r.Message = fmt.Sprintf("%d of %d listeners are not valid", len(ss.listeners)-valid, len(ss.listeners))
		default:
			r.Accepted = true
			r.Reason = string(gwv1.ListenerSetReasonAccepted)
			r.Message = "ListenerSet accepted"
		}
		if r.Accepted {
			attached++
		}
		out = append(out, r)
	}
	return out, attached
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
	// rank is the listener owner whose parentRefs this pass handles.
	rank int
	// rule-level problems, independent of the parent
	unsupported []string
	unresolved  []string
	resolvedWhy gwv1.RouteConditionReason
	// set when no rule could be programmed for unsupported reasons
	nothingUsable bool
}

// mine reports whether this pass handles the parentRef.
func (rc *routeCtx) mine(p gwv1.ParentReference) bool {
	return rc.t.parentRank(p, rc.item.ref.Namespace) == rc.rank
}

// record stores the result for the parentRef at index i.
func (rc *routeCtx) record(i int, pr ParentResult) {
	m := rc.t.parentResults[rc.item.ref]
	if m == nil {
		m = map[int]ParentResult{}
		rc.t.parentResults[rc.item.ref] = m
	}
	m[i] = pr
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

func (t *translator) collectRoutes() []*routeItem {
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
	var routed []*routeItem
	for _, it := range items {
		if t.referencesGateway(it.parents, it.ref.Namespace) {
			routed = append(routed, it)
		}
	}
	return routed
}

// processRank places the routes of one listener owner, oldest first.
// Routes are placed owner by owner, the Gateway first, so that a
// higher-precedence owner's routes take Tailscale Service names, paths and
// ports before routes of later ListenerSets, whatever their age.
func (t *translator) processRank(routed []*routeItem, rank int) {
	for _, it := range routed {
		if t.hasParentOfRank(it, rank) {
			it.process(&routeCtx{t: t, item: it, rank: rank})
		}
	}
}

// assembleRoutes puts each route's parent results in parentRef order.
func (t *translator) assembleRoutes(routed []*routeItem) {
	for _, it := range routed {
		rr := RouteResult{Ref: it.ref}
		for i := range it.parents {
			if pr, ok := t.parentResults[it.ref][i]; ok {
				rr.Parents = append(rr.Parents, pr)
			}
		}
		t.routes = append(t.routes, rr)
	}
}

// parentRank returns the owner rank a parentRef names, or -1 when it names
// neither the Gateway nor one of its allowed ListenerSets. ListenerSets the
// Gateway does not allow are handled with the Gateway (rank 0), which
// only reports them.
func (t *translator) parentRank(p gwv1.ParentReference, routeNS string) int {
	set, ok := t.parentOwner(p, routeNS)
	switch {
	case !ok:
		return -1
	case set == nil || !set.allowed:
		return 0
	}
	return set.rank
}

func (t *translator) hasParentOfRank(it *routeItem, rank int) bool {
	for _, p := range it.parents {
		if t.parentRank(p, it.ref.Namespace) == rank {
			return true
		}
	}
	return false
}

// parentOwner reports whether a parentRef names the Gateway (set is nil)
// or one of the ListenerSets that name the Gateway as their parent.
func (t *translator) parentOwner(p gwv1.ParentReference, routeNS string) (set *setState, ok bool) {
	if p.Group != nil && string(*p.Group) != gwv1.GroupName {
		return nil, false
	}
	ns := routeNS
	if p.Namespace != nil {
		ns = string(*p.Namespace)
	}
	kind := KindGateway
	if p.Kind != nil {
		kind = string(*p.Kind)
	}
	switch kind {
	case KindGateway:
		return nil, ns == t.in.Gateway.Namespace && string(p.Name) == t.in.Gateway.Name
	case KindListenerSet:
		ss := t.sets[types.NamespacedName{Namespace: ns, Name: string(p.Name)}]
		return ss, ss != nil
	}
	return nil, false
}

func (t *translator) parentTargetsGateway(p gwv1.ParentReference, routeNS string) bool {
	_, ok := t.parentOwner(p, routeNS)
	return ok
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
	set, _ := t.parentOwner(p, rc.item.ref.Namespace)
	if set != nil && !set.allowed {
		return nil, gwv1.RouteReasonNotAllowedByListeners, fmt.Sprintf("ListenerSet %s/%s is not allowed by Gateway %s/%s",
			set.ls.Namespace, set.ls.Name, t.in.Gateway.Namespace, t.in.Gateway.Name)
	}
	var candidates []*listenerState
	for _, ls := range t.listeners {
		if ls.set != set {
			continue
		}
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
		return routeNS == ls.ns
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
// While the domain is unknown, any "*.ts.net" name is accepted; once it is
// known, names under another tailnet are rejected.
func ServiceLabel(hostname, tailnetDomain string) (string, error) {
	h := strings.TrimSuffix(strings.ToLower(hostname), ".")
	if strings.HasPrefix(h, "*") {
		return "", fmt.Errorf("wildcard hostname %q cannot name a Tailscale Service", hostname)
	}
	label := h
	if first, rest, ok := strings.Cut(h, "."); ok {
		switch {
		case tailnetDomain != "" && rest == strings.ToLower(tailnetDomain):
		case tailnetDomain != "" && strings.HasSuffix(rest, ".ts.net"):
			return "", fmt.Errorf("hostname %q is not in this tailnet (%s)", hostname, tailnetDomain)
		case tailnetDomain == "" && strings.HasSuffix(rest, ".ts.net"):
			// Accepted until the tailnet domain is known.
		default:
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

// rejectBackend is where requests for an unusable backend go: nothing
// listens there, so they fail instead of falling through to a shorter
// path prefix that belongs to another backend.
func rejectBackend() *backend {
	return &backend{host: "127.0.0.1", port: 1, scheme: "http"}
}

// httpBackend resolves an HTTP-like backend and applies BackendTLSPolicy.
// An unusable backend is reported on the route and mapped to rejectBackend.
func (rc *routeCtx) httpBackend(ref gwv1.BackendObjectReference) *backend {
	b, ok := rc.resolveBackend(ref)
	if !ok {
		return rejectBackend()
	}
	if !rc.t.applyBackendTLS(b) {
		rc.unresolvedf(gwv1.RouteReasonUnsupportedProtocol, "backend Service %s/%s: its BackendTLSPolicy cannot be honored", b.ns, b.name)
		return rejectBackend()
	}
	return b
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
// BackendTLSPolicy in the backend's namespace targets it. It returns false
// when the policy asks for verification that Tailscale serve cannot do: the
// backend must then not be used, rather than reached without verification.
func (t *translator) applyBackendTLS(b *backend) bool {
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
		return true
	}
	key := types.NamespacedName{Namespace: match.Namespace, Name: match.Name}
	v := match.Spec.Validation
	verified := len(v.CACertificateRefs) == 0 && v.WellKnownCACertificates != nil &&
		*v.WellKnownCACertificates == gwv1.WellKnownCACertificatesSystem &&
		strings.EqualFold(string(v.Hostname), b.host) && len(v.SubjectAltNames) == 0
	if verified {
		b.scheme = "https"
		t.policies[key] = &policyState{policy: match, accepted: true,
			message: "TLS to the backend is verified against the system CAs"}
		return true
	}
	t.policies[key] = &policyState{policy: match, message: fmt.Sprintf("Tailscale serve can only verify backend "+
		"certificates against the system CAs (wellKnownCACertificates: System) with the hostname %s, "+
		"and no subjectAltNames; the backend is not used", b.host)}
	return false
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

func policyAccepted(ps *policyState) metav1.Condition {
	if ps.accepted {
		return cond(gwv1.PolicyConditionAccepted, true, gwv1.PolicyReasonAccepted, ps.message, ps.policy.Generation)
	}
	return cond(gwv1.PolicyConditionAccepted, false, gwv1.PolicyReasonInvalid, ps.message, ps.policy.Generation)
}

func (t *translator) policyResults() []PolicyResult {
	var out []PolicyResult
	for key, ps := range t.policies {
		out = append(out, PolicyResult{Ref: key, Conditions: []metav1.Condition{
			policyAccepted(ps),
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
		b := rc.httpBackend(br.BackendObjectReference)
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
		b := rc.httpBackend(br.BackendObjectReference)
		for _, p := range paths {
			mounts = append(mounts, httpMount{path: p, backend: b})
		}
	}
	rc.nothingUsable = len(r.Spec.Rules) > 0 && usableRules == 0
	rc.finishHTTPLike(r.Spec.Hostnames, mounts)
}

func (rc *routeCtx) finishHTTPLike(hostnames []gwv1.Hostname, mounts []httpMount) {
	t := rc.t
	for i, p := range rc.item.parents {
		if !rc.mine(p) {
			continue
		}
		atts, reason, msg := rc.attach(p, hostnames, true)
		if atts == nil {
			rc.record(i, rc.parentResult(p, false, reason, msg, nil))
			continue
		}
		var labelErr string
		var conflicts []string
		placed, attached := 0, 0
		for _, a := range atts {
			hosts := a.hostnames
			var labels []string
			if len(hosts) == 0 && a.ls.set != nil {
				// The Gateway's default Service belongs to the Gateway.
				labelErr = fmt.Sprintf("listener %q of ListenerSet %s/%s has no hostname and the route has none: "+
					"set hostnames on the route or the listener to name a Tailscale Service", a.ls.l.Name, a.ls.set.ls.Namespace, a.ls.set.ls.Name)
				continue
			}
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
					if c := t.addMount(label, rc.rank, uint16(a.ls.l.Port), mode, m, rc.item.ref); c != "" {
						conflicts = append(conflicts, c)
					} else {
						placed++
					}
				}
			}
		}
		if attached == 0 {
			rc.record(i, rc.parentResult(p, false, gwv1.RouteReasonUnsupportedValue, labelErr, nil))
			continue
		}
		extra := conflicts
		if labelErr != "" {
			extra = append(extra, labelErr)
		}
		if len(mounts) > 0 && placed == 0 && len(conflicts) > 0 {
			rc.record(i, rc.parentResult(p, false, ReasonConflicted, strings.Join(dedupe(conflicts), "; "), nil))
			continue
		}
		rc.record(i, rc.parentResult(p, true, "", "", dedupe(extra)))
	}
}

func (t *translator) svc(label string) *svcState {
	s := t.services[label]
	if s == nil {
		s = &svcState{label: label, ports: map[uint16]*portState{}}
		t.services[label] = s
	}
	return s
}

// claimed returns a conflict message when another Gateway serves label.
func (t *translator) claimed(label string) string {
	if gw, ok := t.in.ClaimedServices[label]; ok {
		return fmt.Sprintf("svc:%s is already served by Gateway %s", label, gw)
	}
	return ""
}

// ownLabel takes label for the owner of rank, or returns a conflict
// message when another owner of the Gateway (the Gateway itself or another
// ListenerSet) holds it.
//
// A route that already uses label through one parent may use it through
// another: it adds nothing that is not its own.
func (t *translator) ownLabel(label string, rank int, route RouteRef) string {
	owner, ok := t.labelOwner[label]
	if ok && owner != rank && !t.labelRoutes[label][route] {
		return fmt.Sprintf("svc:%s belongs to %s", label, t.ownerName(owner))
	}
	if !ok {
		t.labelOwner[label] = rank
	}
	if t.labelRoutes[label] == nil {
		t.labelRoutes[label] = map[RouteRef]bool{}
	}
	t.labelRoutes[label][route] = true
	return ""
}

// addMount places an HTTP mount; it returns a conflict message or "".
func (t *translator) addMount(label string, rank int, port uint16, mode string, m httpMount, owner RouteRef) string {
	if c := t.claimed(label); c != "" {
		return c
	}
	if c := t.ownLabel(label, rank, owner); c != "" {
		return c
	}
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
	for i, p := range rc.item.parents {
		if !rc.mine(p) {
			continue
		}
		atts, reason, msg := rc.attach(p, hostnames, useHostnames)
		if atts == nil {
			rc.record(i, rc.parentResult(p, false, reason, msg, nil))
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
				if c := t.addStream(l, rc.rank, uint16(a.ls.l.Port), mode, hostPort(b), rc.item.ref); c != "" {
					conflicts = append(conflicts, c)
				} else {
					placed++
				}
			}
		}
		switch {
		case attached == 0:
			rc.record(i, rc.parentResult(p, false, gwv1.RouteReasonUnsupportedValue, strings.Join(problems, "; "), nil))
		case b != nil && placed == 0 && len(conflicts) > 0:
			rc.record(i, rc.parentResult(p, false, ReasonConflicted, strings.Join(dedupe(conflicts), "; "), nil))
		default:
			rc.record(i, rc.parentResult(p, true, "", "", dedupe(append(conflicts, problems...))))
		}
	}
}

func hostPort(b *backend) string { return b.host + ":" + strconv.Itoa(int(b.port)) }

func (t *translator) addStream(label string, rank int, port uint16, mode, target string, owner RouteRef) string {
	if c := t.claimed(label); c != "" {
		return c
	}
	if c := t.ownLabel(label, rank, owner); c != "" {
		return c
	}
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
	for i, p := range rc.item.parents {
		if !rc.mine(p) {
			continue
		}
		rc.record(i, ParentResult{ParentRef: p, Conditions: []metav1.Condition{
			cond(gwv1.RouteConditionAccepted, false, gwv1.RouteReasonUnsupportedValue,
				"UDPRoute is not supported: Tailscale Services carry TCP only", rc.item.generation),
			cond(gwv1.RouteConditionResolvedRefs, true, gwv1.RouteReasonResolvedRefs, "", rc.item.generation),
		}})
	}
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

// replaceCond replaces the condition of the same type, keeping the order.
func replaceCond(conds *[]metav1.Condition, c metav1.Condition) {
	for i := range *conds {
		if (*conds)[i].Type == c.Type {
			(*conds)[i] = c
			return
		}
	}
	*conds = append(*conds, c)
}

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
