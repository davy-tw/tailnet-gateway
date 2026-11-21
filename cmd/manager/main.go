// Command manager runs the tailnet-gateway controller.
package main

import (
	"context"
	"errors"
	"flag"
	"net/http"
	"os"
	"time"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/discovery"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"

	"go.pkgs.davy.tw/tailnet-gateway/internal/config"
	"go.pkgs.davy.tw/tailnet-gateway/internal/controller"
	"go.pkgs.davy.tw/tailnet-gateway/internal/tailscale"
)

func main() {
	opts := zap.Options{}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	setupLog := ctrl.Log.WithName("setup")

	cfg, err := config.FromEnv()
	if err != nil {
		setupLog.Error(err, "invalid configuration")
		os.Exit(1)
	}

	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(gwv1.Install(scheme))

	restCfg := ctrl.GetConfigOrDie()
	dc, err := discovery.NewDiscoveryClientForConfig(restCfg)
	if err != nil {
		setupLog.Error(err, "creating discovery client")
		os.Exit(1)
	}
	// Wait (without crash-looping) until the Gateway API CRDs are installed.
	// Meanwhile the probe address answers /healthz, so that the liveness
	// probe does not restart the container, and fails /readyz.
	waitProbes := startWaitingProbes(cfg.ProbeAddr)
	var features controller.Features
	for {
		features, err = controller.DetectFeatures(dc)
		if err == nil {
			break
		}
		var missing *controller.ErrMissingCoreCRDs
		if errors.As(err, &missing) {
			setupLog.Error(err, "waiting for Gateway API CRDs; retrying in 30s")
		} else {
			setupLog.Error(err, "discovering Gateway API; retrying in 30s")
		}
		time.Sleep(30 * time.Second)
	}
	if err := waitProbes.Shutdown(context.Background()); err != nil {
		setupLog.Error(err, "stopping the startup probe server")
	}
	for kind, on := range map[string]bool{
		"GRPCRoute": features.GRPCRoute, "TLSRoute": features.TLSRoute, "TCPRoute": features.TCPRoute,
		"UDPRoute": features.UDPRoute, "ReferenceGrant": features.ReferenceGrant, "BackendTLSPolicy": features.BackendTLSPolicy,
	} {
		if !on {
			setupLog.Info("optional Gateway API kind not served at v1; feature disabled", "kind", kind)
		}
	}

	var ts tailscale.API
	if cfg.OAuthEnabled() {
		ts = tailscale.NewOAuthClient(cfg.TailscaleAPIURL, cfg.OAuthClientID, cfg.OAuthClientSecret)
	} else {
		setupLog.Info("WARNING: no Tailscale OAuth client configured; running in manual key mode. " +
			"Each Gateway needs a Secret of the same name with an 'authkey', and devices are not deleted on Gateway deletion.")
	}

	mgr, err := ctrl.NewManager(restCfg, ctrl.Options{
		Scheme:                  scheme,
		Metrics:                 metricsserver.Options{BindAddress: cfg.MetricsAddr},
		HealthProbeBindAddress:  cfg.ProbeAddr,
		LeaderElection:          true,
		LeaderElectionID:        "tailnet-gateway-controller",
		LeaderElectionNamespace: cfg.Namespace,
		// Objects that are only read occasionally are fetched directly
		// instead of caching every one in the cluster.
		Client: client.Options{Cache: &client.CacheOptions{DisableFor: []client.Object{
			&corev1.Pod{}, &corev1.ServiceAccount{}, &rbacv1.Role{}, &rbacv1.RoleBinding{}, &policyv1.PodDisruptionBudget{},
		}}},
	})
	if err != nil {
		setupLog.Error(err, "creating manager")
		os.Exit(1)
	}
	c := mgr.GetClient()
	if err := controller.NewGatewayClassReconciler(c, cfg, mgr.GetEventRecorder("tailnet-gateway")).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "setting up GatewayClass controller")
		os.Exit(1)
	}
	if err := controller.NewGatewayReconciler(c, scheme, cfg, ts, features, mgr.GetEventRecorder("tailnet-gateway")).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "setting up Gateway controller")
		os.Exit(1)
	}
	if err := controller.NewIngressReconciler(c, scheme, cfg, mgr.GetEventRecorder("tailnet-gateway")).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "setting up Ingress controller")
		os.Exit(1)
	}
	if err := controller.NewServiceReconciler(c, scheme, cfg, features, mgr.GetEventRecorder("tailnet-gateway")).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "setting up Service controller")
		os.Exit(1)
	}
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "adding health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "adding ready check")
		os.Exit(1)
	}
	setupLog.Info("starting", "controllerName", cfg.ControllerName, "oauth", cfg.OAuthEnabled())
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "manager exited")
		os.Exit(1)
	}
}

// startWaitingProbes serves the health probes until the manager, which
// serves them afterwards, starts.
func startWaitingProbes(addr string) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "waiting for the Gateway API CRDs", http.StatusServiceUnavailable)
	})
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			ctrl.Log.WithName("setup").Error(err, "startup probe server")
		}
	}()
	return srv
}
