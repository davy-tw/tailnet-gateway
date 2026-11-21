IMG ?= ghcr.io/davy-tw/tailnet-gateway:latest
ENVTEST_K8S_VERSION ?= 1.37.0
ENVTEST_DIR ?= $(CURDIR)/bin/envtest
KUSTOMIZE ?= go run sigs.k8s.io/kustomize/kustomize/v5@v5.7.1
HELM ?= helm
CHART ?= charts/tailnet-gateway

.PHONY: build test test-envtest e2e vet manifests docker-build chart-lint chart-render

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
	go test -tags e2e ./test/e2e/ -v -count=1

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
