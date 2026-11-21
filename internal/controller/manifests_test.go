package controller

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
)

// TestEnvtestManifestsValidate creates the install bundle and every example
// on a real API server with the Gateway API CRDs, so that schema and CEL
// validation catch mistakes.
func TestEnvtestManifestsValidate(t *testing.T) {
	e := startEnv(t, false)
	files := []string{filepath.Join("..", "..", "dist", "install.yaml")}
	examples, _ := filepath.Glob(filepath.Join("..", "..", "examples", "*.yaml"))
	// demo.yaml creates the namespace the other examples use.
	files = append(files, filepath.Join("..", "..", "examples", "demo.yaml"))
	for _, f := range examples {
		if filepath.Base(f) != "demo.yaml" {
			files = append(files, f)
		}
	}
	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		dec := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(data), 4096)
		for {
			u := &unstructured.Unstructured{}
			if err := dec.Decode(&u.Object); err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				t.Fatalf("%s: %v", f, err)
			}
			if len(u.Object) == 0 {
				continue
			}
			if err := e.c.Create(e.ctx, u); err != nil {
				t.Errorf("%s: %s %s: %v", filepath.Base(f), u.GetKind(), u.GetName(), err)
			}
		}
	}
}
