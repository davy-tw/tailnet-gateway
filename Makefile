IMG ?= ghcr.io/davy-tw/tailnet-gateway:latest
ENVTEST_K8S_VERSION ?= 1.37.0
ENVTEST_DIR ?= $(CURDIR)/bin/envtest
KUSTOMIZE ?= go run sigs.k8s.io/kustomize/kustomize/v5@v5.7.1

.PHONY: build test test-envtest e2e vet manifests docker-build

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
