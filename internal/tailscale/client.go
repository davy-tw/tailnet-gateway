// Package tailscale is a small client for the parts of the Tailscale API
// that the controller uses: minting auth keys and deleting devices.
package tailscale

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"golang.org/x/oauth2/clientcredentials"
)

// API is what the controller needs from Tailscale.
type API interface {
	// CreateAuthKey mints a single-use, ephemeral, pre-authorized auth key.
	CreateAuthKey(ctx context.Context, req KeyRequest) (string, error)
	// DeleteDevice deletes a device. A device that does not exist is not
	// an error.
	DeleteDevice(ctx context.Context, deviceID string) error
	// DeviceExists reports whether the device is still known to the tailnet.
	DeviceExists(ctx context.Context, deviceID string) (bool, error)
}

// KeyRequest describes an auth key to mint.
type KeyRequest struct {
	Tags        []string
	Description string
	Expiry      time.Duration
}

// Client talks to the Tailscale API using an OAuth client.
type Client struct {
	baseURL string
	http    *http.Client
}

// NewOAuthClient returns a client that authenticates with OAuth client
// credentials against baseURL (normally https://api.tailscale.com).
func NewOAuthClient(baseURL, clientID, clientSecret string) *Client {
	baseURL = strings.TrimRight(baseURL, "/")
	cc := &clientcredentials.Config{
		ClientID:     clientID,
		ClientSecret: clientSecret,
		TokenURL:     baseURL + "/api/v2/oauth/token",
	}
	hc := cc.Client(context.Background())
	hc.Timeout = 30 * time.Second
	return &Client{baseURL: baseURL, http: hc}
}

// NewClient returns a client that uses hc as is (for tests).
func NewClient(baseURL string, hc *http.Client) *Client {
	return &Client{baseURL: strings.TrimRight(baseURL, "/"), http: hc}
}

// APIError is a non-2xx response from the Tailscale API.
type APIError struct {
	Status  int
	Message string
}

func (e *APIError) Error() string {
	if e.Message == "" {
		return fmt.Sprintf("tailscale API: HTTP %d", e.Status)
	}
	return fmt.Sprintf("tailscale API: HTTP %d: %s", e.Status, e.Message)
}

// IsNotFound reports whether err is a 404 from the API.
func IsNotFound(err error) bool {
	var ae *APIError
	return errors.As(err, &ae) && ae.Status == http.StatusNotFound
}

type keyCreateBody struct {
	Capabilities  keyCapabilities `json:"capabilities"`
	ExpirySeconds int64           `json:"expirySeconds,omitempty"`
	Description   string          `json:"description,omitempty"`
}

type keyCapabilities struct {
	Devices struct {
		Create struct {
			Reusable      bool     `json:"reusable"`
			Ephemeral     bool     `json:"ephemeral"`
			Preauthorized bool     `json:"preauthorized"`
			Tags          []string `json:"tags,omitempty"`
		} `json:"create"`
	} `json:"devices"`
}

// CreateAuthKey implements API.
func (c *Client) CreateAuthKey(ctx context.Context, req KeyRequest) (string, error) {
	var body keyCreateBody
	body.Capabilities.Devices.Create.Ephemeral = true
	body.Capabilities.Devices.Create.Preauthorized = true
	body.Capabilities.Devices.Create.Tags = req.Tags
	body.Description = SanitizeDescription(req.Description)
	if req.Expiry > 0 {
		body.ExpirySeconds = int64(req.Expiry / time.Second)
	}
	var out struct {
		Key string `json:"key"`
	}
	if err := c.do(ctx, http.MethodPost, "/api/v2/tailnet/-/keys", body, &out); err != nil {
		return "", err
	}
	if out.Key == "" {
		return "", errors.New("tailscale API: key creation returned no key")
	}
	return out.Key, nil
}

// DeleteDevice implements API.
func (c *Client) DeleteDevice(ctx context.Context, deviceID string) error {
	err := c.do(ctx, http.MethodDelete, "/api/v2/device/"+url.PathEscape(deviceID), nil, nil)
	if IsNotFound(err) {
		return nil
	}
	return err
}

// DeviceExists implements API.
func (c *Client) DeviceExists(ctx context.Context, deviceID string) (bool, error) {
	err := c.do(ctx, http.MethodGet, "/api/v2/device/"+url.PathEscape(deviceID), nil, nil)
	switch {
	case err == nil:
		return true, nil
	case IsNotFound(err):
		return false, nil
	default:
		return false, err
	}
}

func (c *Client) do(ctx context.Context, method, path string, in, out any) error {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, body)
	if err != nil {
		return err
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("tailscale API: %w", err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		var e struct {
			Message string `json:"message"`
		}
		_ = json.Unmarshal(data, &e)
		return &APIError{Status: resp.StatusCode, Message: e.Message}
	}
	if out != nil && len(data) > 0 {
		if err := json.Unmarshal(data, out); err != nil {
			return fmt.Errorf("tailscale API: decoding response: %w", err)
		}
	}
	return nil
}

// SanitizeDescription keeps the characters the API accepts in a key
// description (letters, digits, spaces and hyphens) and caps the length
// at 50.
func SanitizeDescription(s string) string {
	var b strings.Builder
	for _, r := range s {
		if r < 128 && (r == ' ' || r == '-' || r >= '0' && r <= '9' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z') {
			b.WriteRune(r)
		}
	}
	out := strings.TrimSpace(b.String())
	if len(out) > 50 {
		out = strings.TrimSpace(out[:50])
	}
	return out
}
