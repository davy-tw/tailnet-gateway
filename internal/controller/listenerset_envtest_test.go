package controller

import (
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"

	"go.pkgs.davy.tw/tailnet-gateway/internal/config"
	"go.pkgs.davy.tw/tailnet-gateway/internal/translate"
)

func (e *env) listenerSetCond(ns, name, typ string, status metav1.ConditionStatus, reason string) func() error {
	return func() error {
		ls := &gwv1.ListenerSet{}
		if err := e.get(ns, name, ls); err != nil {
			return err
		}
		if err := condIs(ls.Status.Conditions, typ, status); err != nil {
			return err
		}
		for _, c := range ls.Status.Conditions {
			if c.Type == typ && reason != "" && c.Reason != reason {
				return fmt.Errorf("%s reason = %s (%s), want %s", typ, c.Reason, c.Message, reason)
			}
		}
		return nil
	}
}

// routeParent returns the route's status entry for a parentRef of kind.
func routeParent(r *gwv1.HTTPRoute, kind string) *gwv1.RouteParentStatus {
	for i, p := range r.Status.Parents {
		k := translate.KindGateway
		if p.ParentRef.Kind != nil {
			k = string(*p.ParentRef.Kind)
		}
		if k == kind {
			return &r.Status.Parents[i]
		}
	}
	return nil
}

// TestEnvtestListenerSet attaches a ListenerSet from another namespace to
// a Gateway: refused until the Gateway allows it, then merged into the
// Gateway's serve config, and released when it or the Gateway goes away.
func TestEnvtestListenerSet(t *testing.T) {
	e := startEnv(t, true)
	e.cfg.TailnetDomain = "tail1234.ts.net"
	e.create(namespace("infra"), namespace("team"),
		&gwv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: "tailnet"}, Spec: gwv1.GatewayClassSpec{ControllerName: gwv1.GatewayController(config.DefaultControllerName)}},
		backendService("team", "web", 80))

	gw := httpGateway("infra", "shared")
	ls := &gwv1.ListenerSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team", Name: "apps"},
		Spec: gwv1.ListenerSetSpec{
			ParentRef: gwv1.ParentGatewayReference{Name: "shared", Namespace: ptr.To(gwv1.Namespace("infra"))},
			Listeners: []gwv1.ListenerEntry{{Name: "web", Protocol: gwv1.HTTPProtocolType, Port: 8080}},
		},
	}
	route := hostRoute("team", "app", "apps", "team-app", "web")
	route.Spec.ParentRefs = []gwv1.ParentReference{{Kind: ptr.To(gwv1.Kind(translate.KindListenerSet)), Name: "apps"}}
	e.create(gw, ls, route)

	// Gateways allow no ListenerSets by default.
	e.eventually("ListenerSet not allowed", e.listenerSetCond("team", "apps", "Accepted", metav1.ConditionFalse, "NotAllowed"))
	e.eventually("ListenerSet not programmed", e.listenerSetCond("team", "apps", "Programmed", metav1.ConditionFalse, "NotAllowed"))
	e.eventually("route not allowed", func() error {
		r := &gwv1.HTTPRoute{}
		if err := e.get("team", "app", r); err != nil {
			return err
		}
		p := routeParent(r, translate.KindListenerSet)
		if p == nil {
			return fmt.Errorf("no ListenerSet parent in %+v", r.Status.Parents)
		}
		return condIs(p.Conditions, "Accepted", metav1.ConditionFalse)
	})
	e.eventually("no ListenerSet attached", func() error {
		g := &gwv1.Gateway{}
		if err := e.get("infra", "shared", g); err != nil {
			return err
		}
		if n := ptr.Deref(g.Status.AttachedListenerSets, -1); n != 0 {
			return fmt.Errorf("attachedListenerSets = %d", n)
		}
		return nil
	})

	// Allow ListenerSets from every namespace.
	g := &gwv1.Gateway{}
	if err := e.get("infra", "shared", g); err != nil {
		t.Fatal(err)
	}
	orig := g.DeepCopy()
	g.Spec.AllowedListeners = &gwv1.AllowedListeners{Namespaces: &gwv1.ListenerNamespaces{From: ptr.To(gwv1.NamespacesFromAll)}}
	if err := e.c.Patch(e.ctx, g, client.MergeFrom(orig)); err != nil {
		t.Fatal(err)
	}
	e.eventually("ListenerSet accepted", e.listenerSetCond("team", "apps", "Accepted", metav1.ConditionTrue, "Accepted"))
	// No replica is ready in envtest.
	e.eventually("ListenerSet waits for its Gateway", e.listenerSetCond("team", "apps", "Programmed", metav1.ConditionFalse, "ParentNotProgrammed"))
	e.eventually("attached", func() error {
		g := &gwv1.Gateway{}
		if err := e.get("infra", "shared", g); err != nil {
			return err
		}
		if n := ptr.Deref(g.Status.AttachedListenerSets, -1); n != 1 {
			return fmt.Errorf("attachedListenerSets = %d", n)
		}
		if len(g.Status.Listeners) != 1 || g.Status.Listeners[0].Name != "http" {
			return fmt.Errorf("gateway listeners = %+v", g.Status.Listeners)
		}
		return nil
	})
	e.eventually("route accepted through the ListenerSet", func() error {
		r := &gwv1.HTTPRoute{}
		if err := e.get("team", "app", r); err != nil {
			return err
		}
		p := routeParent(r, translate.KindListenerSet)
		if p == nil || len(r.Status.Parents) != 1 {
			return fmt.Errorf("parents = %+v", r.Status.Parents)
		}
		return condIs(p.Conditions, "Accepted", metav1.ConditionTrue)
	})
	e.eventually("listener status", func() error {
		l := &gwv1.ListenerSet{}
		if err := e.get("team", "apps", l); err != nil {
			return err
		}
		if len(l.Status.Listeners) != 1 || l.Status.Listeners[0].AttachedRoutes != 1 {
			return fmt.Errorf("listeners = %+v", l.Status.Listeners)
		}
		return condIs(l.Status.Listeners[0].Conditions, "Accepted", metav1.ConditionTrue)
	})
	e.eventually("served by the Gateway's proxy", func() error {
		sc, err := e.serveConfig("infra", "shared")
		if err != nil {
			return err
		}
		svc := sc.Services["svc:team-app"]
		if svc == nil || svc.TCP[8080] == nil || !svc.TCP[8080].HTTP {
			return fmt.Errorf("serve config = %+v", sc.Services)
		}
		return nil
	})

	// Removing the ListenerSet withdraws its listeners and the route's
	// status entry.
	if err := e.c.Delete(e.ctx, ls); err != nil {
		t.Fatal(err)
	}
	e.eventually("listeners withdrawn", func() error {
		sc, err := e.serveConfig("infra", "shared")
		if err != nil {
			return err
		}
		if _, ok := sc.Services["svc:team-app"]; ok {
			return fmt.Errorf("svc:team-app still served")
		}
		g := &gwv1.Gateway{}
		if err := e.get("infra", "shared", g); err != nil {
			return err
		}
		if n := ptr.Deref(g.Status.AttachedListenerSets, -1); n != 0 {
			return fmt.Errorf("attachedListenerSets = %d", n)
		}
		return nil
	})
	e.eventually("stale route status removed", func() error {
		r := &gwv1.HTTPRoute{}
		if err := e.get("team", "app", r); err != nil {
			return err
		}
		if len(r.Status.Parents) != 0 {
			return fmt.Errorf("parents = %+v", r.Status.Parents)
		}
		return nil
	})

	// A ListenerSet is released when its Gateway is deleted.
	again := &gwv1.ListenerSet{ObjectMeta: metav1.ObjectMeta{Namespace: "team", Name: "again"}, Spec: ls.Spec}
	e.create(again)
	e.eventually("second ListenerSet accepted", e.listenerSetCond("team", "again", "Accepted", metav1.ConditionTrue, ""))
	if err := e.c.Delete(e.ctx, &gwv1.Gateway{ObjectMeta: metav1.ObjectMeta{Namespace: "infra", Name: "shared"}}); err != nil {
		t.Fatal(err)
	}
	e.eventually("ListenerSet released", e.listenerSetCond("team", "again", "Accepted", metav1.ConditionFalse, "ParentNotAccepted"))
}

// TestEnvtestListenerSetSelectorAndMove covers allowedListeners with a
// namespace selector (a relabeled namespace is no longer allowed) and a
// ListenerSet whose parentRef moves to a Gateway that does not exist.
func TestEnvtestListenerSetSelectorAndMove(t *testing.T) {
	e := startEnv(t, true)
	e.cfg.TailnetDomain = "tail1234.ts.net"
	team := namespace("team")
	team.Labels = map[string]string{"gateway-access": "shared"}
	e.create(namespace("infra"), team,
		&gwv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: "tailnet"}, Spec: gwv1.GatewayClassSpec{ControllerName: gwv1.GatewayController(config.DefaultControllerName)}},
		backendService("team", "web", 80))
	gw := httpGateway("infra", "shared")
	gw.Spec.AllowedListeners = &gwv1.AllowedListeners{Namespaces: &gwv1.ListenerNamespaces{
		From:     ptr.To(gwv1.NamespacesFromSelector),
		Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"gateway-access": "shared"}},
	}}
	ls := &gwv1.ListenerSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team", Name: "apps"},
		Spec: gwv1.ListenerSetSpec{
			ParentRef: gwv1.ParentGatewayReference{Name: "shared", Namespace: ptr.To(gwv1.Namespace("infra"))},
			Listeners: []gwv1.ListenerEntry{{Name: "web", Protocol: gwv1.HTTPProtocolType, Port: 8080}},
		},
	}
	route := hostRoute("team", "app", "apps", "team-app", "web")
	route.Spec.ParentRefs = []gwv1.ParentReference{{Kind: ptr.To(gwv1.Kind(translate.KindListenerSet)), Name: "apps"}}
	e.create(gw, ls, route)

	served := func(want bool) func() error {
		return func() error {
			sc, err := e.serveConfig("infra", "shared")
			if err != nil {
				return err
			}
			if _, ok := sc.Services["svc:team-app"]; ok != want {
				return fmt.Errorf("svc:team-app served = %v, want %v", ok, want)
			}
			return nil
		}
	}
	e.eventually("selected namespace allowed", e.listenerSetCond("team", "apps", "Accepted", metav1.ConditionTrue, "Accepted"))
	e.eventually("served", served(true))

	// Relabel the namespace: the selector no longer matches.
	ns := &corev1.Namespace{}
	if err := e.get("", "team", ns); err != nil {
		t.Fatal(err)
	}
	orig := ns.DeepCopy()
	ns.Labels["gateway-access"] = "none"
	if err := e.c.Patch(e.ctx, ns, client.MergeFrom(orig)); err != nil {
		t.Fatal(err)
	}
	e.eventually("relabeled namespace not allowed", e.listenerSetCond("team", "apps", "Accepted", metav1.ConditionFalse, "NotAllowed"))
	e.eventually("withdrawn", served(false))

	// Allow it again, then move the ListenerSet to a Gateway that does not
	// exist.
	_ = e.get("", "team", ns)
	orig = ns.DeepCopy()
	ns.Labels["gateway-access"] = "shared"
	if err := e.c.Patch(e.ctx, ns, client.MergeFrom(orig)); err != nil {
		t.Fatal(err)
	}
	e.eventually("allowed again", e.listenerSetCond("team", "apps", "Accepted", metav1.ConditionTrue, "Accepted"))
	e.eventually("recorded on the Gateway", func() error {
		g := &gwv1.Gateway{}
		if err := e.get("infra", "shared", g); err != nil {
			return err
		}
		if v := g.Annotations[config.AnnotationListenerSets]; v != "team/apps" {
			return fmt.Errorf("annotation = %q", v)
		}
		return nil
	})
	cur := &gwv1.ListenerSet{}
	if err := e.get("team", "apps", cur); err != nil {
		t.Fatal(err)
	}
	origLS := cur.DeepCopy()
	cur.Spec.ParentRef.Name = "missing"
	if err := e.c.Patch(e.ctx, cur, client.MergeFrom(origLS)); err != nil {
		t.Fatal(err)
	}
	e.eventually("released after the move", e.listenerSetCond("team", "apps", "Accepted", metav1.ConditionFalse, "ParentNotAccepted"))
	e.eventually("moved listeners withdrawn", served(false))
	e.eventually("route status removed", func() error {
		r := &gwv1.HTTPRoute{}
		if err := e.get("team", "app", r); err != nil {
			return err
		}
		if len(r.Status.Parents) != 0 {
			return fmt.Errorf("parents = %+v", r.Status.Parents)
		}
		return nil
	})

	// The Gateway moves to another controller's class: its ListenerSets go
	// back to waiting for a controller.
	stay := &gwv1.ListenerSet{ObjectMeta: metav1.ObjectMeta{Namespace: "team", Name: "stay"}, Spec: ls.Spec}
	e.create(stay, &gwv1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: "other"},
		Spec: gwv1.GatewayClassSpec{ControllerName: "example.com/other-controller"}})
	e.eventually("stay accepted", e.listenerSetCond("team", "stay", "Accepted", metav1.ConditionTrue, ""))
	g := &gwv1.Gateway{}
	if err := e.get("infra", "shared", g); err != nil {
		t.Fatal(err)
	}
	origGW := g.DeepCopy()
	g.Spec.GatewayClassName = "other"
	if err := e.c.Patch(e.ctx, g, client.MergeFrom(origGW)); err != nil {
		t.Fatal(err)
	}
	e.eventually("handed back", e.listenerSetCond("team", "stay", "Accepted", metav1.ConditionUnknown, "Pending"))
}
