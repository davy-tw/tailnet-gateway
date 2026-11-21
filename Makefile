IMG ?= ghcr.io/davy-tw/tailnet-gateway/controller:latest
ENVTEST_K8S_VERSION ?= 1.37.0
ENVTEST_DIR ?= $(CURDIR)/bin/envtest
KUSTOMIZE ?= go run sigs.k8s.io/kustomize/kustomize/v5@v5.7.1
HELM ?= helm
CHART ?= charts/tailnet-gateway

.PHONY: build test test-envtest e2e e2e-kind e2e-k3d e2e-install e2e-diagnostics vet manifests docker-build chart-lint chart-render

build:
	go build -o bin/manager ./cmd/manager

vet:
	go vet ./...
	go vet -tags e2e ./test/e2e/

# Unit tests only (envtest tests skip without KUBEBUILDER_ASSETS).
test:
	go test ./...

# Unit and integration tests against a real kube-apiserver and etcd.
test-envtest: manifests
	KUBEBUILDER_ASSETS="$$(go run sigs.k8s.io/controller-runtime/tools/setup-envtest@release-0.22 use $(ENVTEST_K8S_VERSION) --bin-dir $(ENVTEST_DIR) -p path)" \
		go test ./... -count=1

# End-to-end tests against the current kubeconfig context.
e2e:
	go test -tags e2e ./test/e2e/ -v -count=1 -timeout 20m

# End-to-end tests on a throwaway kind or k3d (k3s) cluster, without a
# tailnet: the controller is installed from the chart with the image built
# here, in manual key mode, and the tests use a fake auth key. The cluster is
# deleted afterwards unless E2E_KEEP=1.
KIND ?= kind
K3D ?= k3d
KIND_CLUSTER ?= tailnet-gateway-e2e
K3D_CLUSTER ?= tailnet-gateway-e2e
# k3s's bundled Traefik is disabled: the tests do not need an ingress
# controller, and it would only compete for the Gateway API CRDs.
# K3S_IMAGE pins the k3s version the k3d cluster runs (a k3d built from
# source has no default and falls back to an old one).
K3S_IMAGE ?= rancher/k3s:v1.37.2-k3s1
GATEWAY_API_VERSION ?= v1.6.3
E2E_IMG ?= ghcr.io/davy-tw/tailnet-gateway/controller:e2e
E2E_ENV = E2E_AUTHKEY=tskey-auth-e2e-not-a-real-key

e2e-kind:
	@command -v $(KIND) >/dev/null || { echo "kind is not installed"; exit 1; }
	docker build -t $(E2E_IMG) .
	$(KIND) get clusters | grep -qx $(KIND_CLUSTER) || $(KIND) create cluster --name $(KIND_CLUSTER) --wait 120s
	$(KIND) load docker-image $(E2E_IMG) --name $(KIND_CLUSTER)
	kubectl config use-context kind-$(KIND_CLUSTER)
	{ $(MAKE) e2e-install && $(E2E_ENV) $(MAKE) e2e; } || { $(MAKE) e2e-diagnostics; [ "$(E2E_KEEP)" = 1 ] || $(KIND) delete cluster --name $(KIND_CLUSTER); exit 1; }
	[ "$(E2E_KEEP)" = 1 ] || $(KIND) delete cluster --name $(KIND_CLUSTER)

e2e-k3d:
	@command -v $(K3D) >/dev/null || { echo "k3d is not installed"; exit 1; }
	docker build -t $(E2E_IMG) .
	$(K3D) cluster get $(K3D_CLUSTER) >/dev/null 2>&1 || $(K3D) cluster create $(K3D_CLUSTER) --image $(K3S_IMAGE) \
		--k3s-arg "--disable=traefik@server:*" --wait --timeout 5m
	$(K3D) image import $(E2E_IMG) --cluster $(K3D_CLUSTER)
	kubectl config use-context k3d-$(K3D_CLUSTER)
	{ $(MAKE) e2e-install && $(E2E_ENV) $(MAKE) e2e; } || { $(MAKE) e2e-diagnostics; [ "$(E2E_KEEP)" = 1 ] || $(K3D) cluster delete $(K3D_CLUSTER); exit 1; }
	[ "$(E2E_KEEP)" = 1 ] || $(K3D) cluster delete $(K3D_CLUSTER)

# Installs the Gateway API CRDs and the chart into the current context.
# --force-conflicts: on a k3s cluster that runs its bundled Traefik (k3d
# disables it above), Traefik may already have applied Gateway API CRDs.
e2e-install:
	kubectl apply --server-side --force-conflicts -f https://github.com/kubernetes-sigs/gateway-api/releases/download/$(GATEWAY_API_VERSION)/standard-install.yaml
	kubectl wait --for condition=Established crd --all --timeout 60s
	$(HELM) upgrade --install tailnet-gateway $(CHART) --namespace tailnet-gateway-system --create-namespace \
		--set image.repository=$(firstword $(subst :, ,$(E2E_IMG))) --set image.tag=$(lastword $(subst :, ,$(E2E_IMG))) \
		--set image.pullPolicy=Never --set proxy.drainGracePeriod=5s --wait --timeout 5m

e2e-diagnostics:
	-kubectl get gatewayclasses,gateways,listenersets,httproutes,tcproutes,ingresses,statefulsets,pods -A -o wide
	-kubectl -n tailnet-gateway-system describe pods
	-kubectl -n tailnet-gateway-system logs deployment/tailnet-gateway --all-containers --tail=300
	-kubectl get events -A --sort-by=.lastTimestamp | tail -80

manifests:
	$(KUSTOMIZE) build config/default > dist/install.yaml

docker-build:
	docker build -t $(IMG) .

chart-lint:
	$(HELM) lint $(CHART)

# Renders the chart with every optional object enabled, for the tests in
# internal/controller/chart_test.go (CHART_MANIFESTS=$(CURDIR)/bin/chart.yaml).
chart-render:
	mkdir -p bin
	$(HELM) template tailnet-gateway $(CHART) --namespace tailnet-gateway-system \
		--set oauth.clientId=id --set oauth.clientSecret=secret --set proxy.tags=tag:k8s \
		--set ingressClass.default=true > bin/chart.yaml
