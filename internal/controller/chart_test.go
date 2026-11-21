package controller

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
)

// chartManifests returns the objects of the rendered Helm chart named by
// CHART_MANIFESTS (see `make chart-render`), or skips the test.
func chartManifests(t *testing.T) []*unstructured.Unstructured {
	t.Helper()
	path := os.Getenv("CHART_MANIFESTS")
	if path == "" {
		t.Skip("CHART_MANIFESTS not set; run `make chart-render` and set it to the output")
	}
	return decodeManifests(t, path)
}

func decodeManifests(t *testing.T, path string) []*unstructured.Unstructured {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var out []*unstructured.Unstructured
	dec := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(data), 4096)
	for {
		u := &unstructured.Unstructured{}
		if err := dec.Decode(&u.Object); err != nil {
			if errors.Is(err, io.EOF) {
				return out
			}
			t.Fatalf("%s: %v", path, err)
		}
		if len(u.Object) > 0 {
			out = append(out, u)
		}
	}
}

func clusterRoleRules(t *testing.T, objs []*unstructured.Unstructured) any {
	t.Helper()
	for _, u := range objs {
		if u.GetKind() == "ClusterRole" {
			return u.Object["rules"]
		}
	}
	t.Fatal("no ClusterRole")
	return nil
}

// TestChartRBACInSync checks that the chart grants the controller what the
// kustomize install bundle grants.
func TestChartRBACInSync(t *testing.T) {
	chart := clusterRoleRules(t, chartManifests(t))
	bundle := clusterRoleRules(t, decodeManifests(t, filepath.Join("..", "..", "dist", "install.yaml")))
	if !reflect.DeepEqual(chart, bundle) {
		t.Errorf("chart ClusterRole rules differ from config/rbac/cluster_role.yaml:\nchart:  %v\nbundle: %v", chart, bundle)
	}
}

// TestEnvtestChartValidates creates the rendered chart on a real API server
// with the Gateway API CRDs.
func TestEnvtestChartValidates(t *testing.T) {
	objs := chartManifests(t)
	e := startEnv(t, false)
	namespaces := map[string]bool{}
	for _, u := range objs {
		if ns := u.GetNamespace(); ns != "" && !namespaces[ns] {
			namespaces[ns] = true
			e.create(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}})
		}
	}
	for _, u := range objs {
		if err := e.c.Create(e.ctx, u); err != nil {
			t.Errorf("%s %s: %v", u.GetKind(), u.GetName(), err)
		}
	}
}
