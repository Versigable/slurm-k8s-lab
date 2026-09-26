package slurm

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// RESTClient reads a Slurm cluster through slurmrestd, e.g. Slinky's
// slurm-restapi Service. The JSON is the same data_parser output the CLIs
// print with --json, so it decodes into the same types.
//
// It holds two JWTs (Slurm's auth/jwt, minted with `scontrol token`):
//   - a read token for an unprivileged user, used for everything it reads;
//   - an optional admin token, used only for drain and resume. Without
//     accounting there are no Slurm admin levels, so node updates need the
//     SlurmUser; keeping that token separate means reads never carry it.
//
// The Slurm user name comes from each token's own "sun" claim.
type RESTClient struct {
	BaseURL    string // e.g. http://10.0.5.143:6820
	APIVersion string // e.g. v0.0.45
	Read       Token
	Admin      *Token // nil: writes unavailable
	HTTP       *http.Client
}

// Token is a Slurm JWT and the user it was issued for.
type Token struct {
	User  string
	Value string
}

// NewRESTFromDir builds a client from a directory holding url, api-version
// (optional), token-read and token-admin (optional); written by ansible/triage.yml.
func NewRESTFromDir(dir string) (*RESTClient, error) {
	read := func(name string) (string, error) {
		b, err := os.ReadFile(filepath.Join(dir, name))
		return strings.TrimSpace(string(b)), err
	}
	base, err := read("url")
	if err != nil {
		return nil, err
	}
	c := &RESTClient{BaseURL: strings.TrimRight(base, "/"), APIVersion: "v0.0.45", HTTP: &http.Client{Timeout: 15 * time.Second}}
	if v, err := read("api-version"); err == nil && v != "" {
		c.APIVersion = v
	}
	rt, err := read("token-read")
	if err != nil {
		return nil, err
	}
	if c.Read, err = ParseToken(rt); err != nil {
		return nil, fmt.Errorf("token-read: %w", err)
	}
	if at, err := read("token-admin"); err == nil && at != "" {
		t, err := ParseToken(at)
		if err != nil {
			return nil, fmt.Errorf("token-admin: %w", err)
		}
		c.Admin = &t
	}
	return c, nil
}

// ParseToken reads the Slurm user name ("sun" claim) out of a Slurm JWT.
// It does not verify the signature: slurmctld does that on every request.
func ParseToken(jwt string) (Token, error) {
	parts := strings.Split(jwt, ".")
	if len(parts) != 3 {
		return Token{}, errors.New("not a JWT")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return Token{}, fmt.Errorf("JWT payload: %w", err)
	}
	var claims struct {
		User string `json:"sun"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil || claims.User == "" {
		return Token{}, errors.New("JWT has no Slurm user (sun) claim")
	}
	return Token{User: claims.User, Value: jwt}, nil
}

type restEnvelope struct {
	Errors []struct {
		Error       string `json:"error"`
		Description string `json:"description"`
	} `json:"errors"`
}

func (c *RESTClient) do(ctx context.Context, tok Token, method, path string, body any, v any) error {
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+"/slurm/"+c.APIVersion+path, rdr)
	if err != nil {
		return err
	}
	req.Header.Set("X-SLURM-USER-NAME", tok.User)
	req.Header.Set("X-SLURM-USER-TOKEN", tok.Value)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	// slurmrestd reports failures in an errors array, sometimes with HTTP 200.
	var env restEnvelope
	_ = json.Unmarshal(data, &env)
	if len(env.Errors) > 0 {
		e := env.Errors[0]
		return fmt.Errorf("%s %s: %s", method, path, strings.TrimSpace(e.Error+" "+e.Description))
	}
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("%s %s: %s", method, path, resp.Status)
	}
	if v == nil {
		return nil
	}
	return json.Unmarshal(data, v)
}

func (c *RESTClient) Nodes(ctx context.Context) ([]Node, error) {
	var resp struct {
		Nodes []Node `json:"nodes"`
	}
	err := c.do(ctx, c.Read, http.MethodGet, "/nodes/", nil, &resp)
	return resp.Nodes, err
}

func (c *RESTClient) Jobs(ctx context.Context) ([]Job, error) {
	var resp struct {
		Jobs []Job `json:"jobs"`
	}
	err := c.do(ctx, c.Read, http.MethodGet, "/jobs/", nil, &resp)
	return resp.Jobs, err
}

// History and Events need slurmdbd; the Slinky cluster here runs without
// accounting, so there is nothing to read. (With accounting enabled these
// would come from /slurmdb/<version>/jobs and events.)
func (c *RESTClient) History(context.Context, time.Duration) ([]HistoricalJob, error) {
	return nil, nil
}
func (c *RESTClient) Events(context.Context, time.Duration) ([]Event, error) { return nil, nil }

// Drain and Resume never send "comment": the Slinky operator owns it.
func (c *RESTClient) Drain(ctx context.Context, node, reason string) error {
	if strings.TrimSpace(reason) == "" {
		return errors.New("a drain reason is required")
	}
	return c.update(ctx, node, map[string]any{"state": []string{"DRAIN"}, "reason": reason})
}

func (c *RESTClient) Resume(ctx context.Context, node string) error {
	return c.update(ctx, node, map[string]any{"state": []string{"RESUME"}})
}

func (c *RESTClient) update(ctx context.Context, node string, body map[string]any) error {
	if c.Admin == nil {
		return errors.New("no admin token configured for this Slurm cluster; node updates are unavailable")
	}
	return c.do(ctx, *c.Admin, http.MethodPost, "/node/"+url.PathEscape(node), body, nil)
}
