package tailscale

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"
)

func TestCreateAuthKey(t *testing.T) {
	var got keyCreateBody
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/v2/tailnet/-/keys" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Error(err)
		}
		w.Write([]byte(`{"id":"k1","key":"tskey-auth-xyz"}`))
	}))
	defer srv.Close()
	c := NewClient(srv.URL, srv.Client())
	key, err := c.CreateAuthKey(context.Background(), KeyRequest{Tags: []string{"tag:a"}, Description: "gw: default/web!", Expiry: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	if key != "tskey-auth-xyz" {
		t.Errorf("key = %q", key)
	}
	cr := got.Capabilities.Devices.Create
	if cr.Reusable || !cr.Ephemeral || !cr.Preauthorized || !reflect.DeepEqual(cr.Tags, []string{"tag:a"}) {
		t.Errorf("capabilities = %+v", cr)
	}
	if got.Description != "gw defaultweb" || got.ExpirySeconds != 3600 {
		t.Errorf("body = %+v", got)
	}
}

func TestCreateAuthKeyError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"message":"requested tags are invalid or not permitted"}`))
	}))
	defer srv.Close()
	_, err := NewClient(srv.URL, srv.Client()).CreateAuthKey(context.Background(), KeyRequest{})
	if err == nil || err.Error() != "tailscale API: HTTP 400: requested tags are invalid or not permitted" {
		t.Fatalf("err = %v", err)
	}
}

func TestDevices(t *testing.T) {
	devices := map[string]bool{"n1": true}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.URL.Path[len("/api/v2/device/"):]
		if !devices[id] {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.Method == http.MethodDelete {
			delete(devices, id)
		}
		w.Write([]byte(`{}`))
	}))
	defer srv.Close()
	c := NewClient(srv.URL, srv.Client())
	ctx := context.Background()
	if ok, err := c.DeviceExists(ctx, "n1"); !ok || err != nil {
		t.Fatalf("exists = %v, %v", ok, err)
	}
	if err := c.DeleteDevice(ctx, "n1"); err != nil {
		t.Fatal(err)
	}
	if ok, err := c.DeviceExists(ctx, "n1"); ok || err != nil {
		t.Fatalf("exists after delete = %v, %v", ok, err)
	}
	if err := c.DeleteDevice(ctx, "n1"); err != nil {
		t.Fatalf("deleting a missing device should succeed: %v", err)
	}
}

func TestSanitizeDescription(t *testing.T) {
	long := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	if got := SanitizeDescription(long); len(got) != 50 {
		t.Errorf("len = %d", len(got))
	}
	if got := SanitizeDescription("tailnet-gateway proxy é"); got != "tailnet-gateway proxy" {
		t.Errorf("got %q", got)
	}
}
