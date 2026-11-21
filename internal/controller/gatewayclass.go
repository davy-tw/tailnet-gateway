package controller

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"

	"go.pkgs.davy.tw/tailnet-gateway/internal/config"
)

// classResolver looks up managed GatewayClasses and their parameters.
type classResolver struct {
	client.Client
	Config *config.Config
}

// managedClass returns the GatewayClass if this controller serves it.
func (r *classResolver) managedClass(ctx context.Context, name string) (*gwv1.GatewayClass, error) {
	gc := &gwv1.GatewayClass{}
	if err := r.Get(ctx, types.NamespacedName{Name: name}, gc); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	if string(gc.Spec.ControllerName) != r.Config.ControllerName {
		return nil, nil
	}
	return gc, nil
}

// invalidParamsError marks a GatewayClass whose parameters are unusable.
type invalidParamsError struct{ msg string }

func (e *invalidParamsError) Error() string { return e.msg }

// classParams resolves a class's parameters on top of the controller
// defaults. An *invalidParamsError means the class is invalid; other errors
// are transient.
func (r *classResolver) classParams(ctx context.Context, gc *gwv1.GatewayClass) (config.ClassParams, []string, error) {
	ref := gc.Spec.ParametersRef
	if ref == nil {
		return r.Config.Defaults, nil, nil
	}
	if string(ref.Group) != "" || string(ref.Kind) != "ConfigMap" {
		return config.ClassParams{}, nil, &invalidParamsError{fmt.Sprintf("parametersRef must be a core ConfigMap, not %s/%s", ref.Group, ref.Kind)}
	}
	if ref.Namespace == nil || *ref.Namespace == "" {
		return config.ClassParams{}, nil, &invalidParamsError{"parametersRef must set a namespace"}
	}
	cm := &corev1.ConfigMap{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: string(*ref.Namespace), Name: ref.Name}, cm); err != nil {
		if apierrors.IsNotFound(err) {
			return config.ClassParams{}, nil, &invalidParamsError{fmt.Sprintf("parameters ConfigMap %s/%s not found", *ref.Namespace, ref.Name)}
		}
		return config.ClassParams{}, nil, err
	}
	p, unknown, err := config.ParseClassParams(cm.Data, r.Config.Defaults)
	if err != nil {
		return config.ClassParams{}, nil, &invalidParamsError{fmt.Sprintf("parameters ConfigMap %s/%s: %v", *ref.Namespace, ref.Name, err)}
	}
	return p, unknown, nil
}

// GatewayClassReconciler sets Accepted on managed GatewayClasses.
type GatewayClassReconciler struct {
	classResolver
	Recorder events.EventRecorder
}

// NewGatewayClassReconciler returns a reconciler.
func NewGatewayClassReconciler(c client.Client, cfg *config.Config, rec events.EventRecorder) *GatewayClassReconciler {
	return &GatewayClassReconciler{classResolver: classResolver{Client: c, Config: cfg}, Recorder: rec}
}

// Reconcile implements reconcile.Reconciler.
func (r *GatewayClassReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	gc, err := r.managedClass(ctx, req.Name)
	if err != nil || gc == nil {
		return ctrl.Result{}, err
	}
	_, unknown, perr := r.classParams(ctx, gc)
	var ip *invalidParamsError
	cond := metav1.Condition{
		Type:               string(gwv1.GatewayClassConditionStatusAccepted),
		Status:             metav1.ConditionTrue,
		Reason:             string(gwv1.GatewayClassReasonAccepted),
		Message:            "GatewayClass accepted",
		ObservedGeneration: gc.Generation,
	}
	switch {
	case perr == nil:
	case asInvalid(perr, &ip):
		cond.Status = metav1.ConditionFalse
		cond.Reason = string(gwv1.GatewayClassReasonInvalidParameters)
		cond.Message = ip.msg
	default:
		return ctrl.Result{}, perr
	}
	if len(unknown) > 0 {
		log.FromContext(ctx).Info("ignoring unknown GatewayClass parameters", "keys", unknown)
		if r.Recorder != nil {
			r.Recorder.Eventf(gc, nil, corev1.EventTypeWarning, "UnknownParameters", "Validate",
				"ignoring unknown parameter keys: %s", strings.Join(unknown, ", "))
		}
	}
	orig := gc.DeepCopy()
	meta.SetStatusCondition(&gc.Status.Conditions, cond)
	if equalConditions(orig.Status.Conditions, gc.Status.Conditions) {
		return ctrl.Result{}, nil
	}
	return ctrl.Result{}, r.Status().Patch(ctx, gc, client.MergeFrom(orig))
}

func asInvalid(err error, target **invalidParamsError) bool {
	ip, ok := err.(*invalidParamsError)
	if ok {
		*target = ip
	}
	return ok
}

func equalConditions(a, b []metav1.Condition) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		x, y := a[i], b[i]
		if x.Type != y.Type || x.Status != y.Status || x.Reason != y.Reason || x.Message != y.Message || x.ObservedGeneration != y.ObservedGeneration {
			return false
		}
	}
	return true
}

// classesForConfigMap maps a parameters ConfigMap to the classes using it.
func classesForConfigMap(c client.Client, controllerName string) handler.MapFunc {
	return func(ctx context.Context, obj client.Object) []reconcile.Request {
		var list gwv1.GatewayClassList
		if err := c.List(ctx, &list); err != nil {
			return nil
		}
		var out []reconcile.Request
		for _, gc := range list.Items {
			if string(gc.Spec.ControllerName) != controllerName {
				continue
			}
			if ref := gc.Spec.ParametersRef; ref != nil && string(ref.Kind) == "ConfigMap" && ref.Name == obj.GetName() &&
				ref.Namespace != nil && string(*ref.Namespace) == obj.GetNamespace() {
				out = append(out, reconcile.Request{NamespacedName: types.NamespacedName{Name: gc.Name}})
			}
		}
		return out
	}
}

// SetupWithManager registers the reconciler.
func (r *GatewayClassReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("gatewayclass").
		For(&gwv1.GatewayClass{}, builder.WithPredicates(predicate.NewPredicateFuncs(func(o client.Object) bool {
			gc, ok := o.(*gwv1.GatewayClass)
			return ok && string(gc.Spec.ControllerName) == r.Config.ControllerName
		}))).
		Watches(&corev1.ConfigMap{}, handler.EnqueueRequestsFromMapFunc(classesForConfigMap(r.Client, r.Config.ControllerName))).
		Complete(r)
}
