package slurm

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// fakeJWT builds an unsigned JWT with a Slurm "sun" claim (slurmctld verifies
// signatures; the client only reads the user name).
func fakeJWT(user string) string {
	enc := base64.RawURLEncoding.EncodeToString
	claims, _ := json.Marshal(map[string]any{"sun": user, "exp": 1})
	return enc([]byte(`{"alg":"HS256"}`)) + "." + enc(claims) + ".sig"
}

func TestParseToken(t *testing.T) {
	tok, err := ParseToken(fakeJWT("nobody"))
	if err != nil || tok.User != "nobody" {
		t.Fatalf("got %+v, %v", tok, err)
	}
	for _, bad := range []string{"", "a.b", "x.!!!.y", fakeJWT("")} {
		if _, err := ParseToken(bad); err == nil {
			t.Errorf("ParseToken(%q) should fail", bad)
		}
	}
}

func TestRESTClient(t *testing.T) {
	type call struct{ method, path, user, body string }
	var calls []call
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		calls = append(calls, call{r.Method, r.URL.Path, r.Header.Get("X-SLURM-USER-NAME"), string(b)})
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/slurm/v0.0.45/nodes/":
			io.WriteString(w, `{"nodes":[{"name":"slinky-0","state":["IDLE"],"comment":"{\"podName\":\"p\",\"node\":\"k8s-w1\"}"}],"errors":[]}`)
		case r.Method == http.MethodGet && r.URL.Path == "/slurm/v0.0.45/jobs/":
			io.WriteString(w, `{"jobs":[],"errors":[]}`)
		case r.Method == http.MethodPost && r.URL.Path == "/slurm/v0.0.45/node/slinky-0":
			if r.Header.Get("X-SLURM-USER-NAME") != "slurm" {
				// what slurmctld actually answers a non-admin: HTTP 200 + errors
				io.WriteString(w, `{"errors":[{"error":"Invalid user id","description":"update failed"}]}`)
				return
			}
			io.WriteString(w, `{"errors":[]}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	read, _ := ParseToken(fakeJWT("nobody"))
	admin, _ := ParseToken(fakeJWT("slurm"))
	c := &RESTClient{BaseURL: srv.URL, APIVersion: "v0.0.45", Read: read, Admin: &admin, HTTP: srv.Client()}
	ctx := context.Background()

	nodes, err := c.Nodes(ctx)
	if err != nil || len(nodes) != 1 {
		t.Fatalf("nodes: %v %v", nodes, err)
	}
	if p, ok := nodes[0].SlinkyPod(); !ok || p.Node != "k8s-w1" {
		t.Errorf("pod link lost in decoding: %+v", nodes[0])
	}
	if _, err := c.Jobs(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.Drain(ctx, "slinky-0", "triage: disk"); err != nil {
		t.Fatal(err)
	}
	if err := c.Resume(ctx, "slinky-0"); err != nil {
		t.Fatal(err)
	}

	for _, cl := range calls {
		want := "nobody"
		if cl.method == http.MethodPost {
			want = "slurm"
			if strings.Contains(cl.body, "comment") {
				t.Errorf("node updates must never touch the operator's comment: %s", cl.body)
			}
		}
		if cl.user != want {
			t.Errorf("%s %s sent as %q, want %q (reads use the read token, writes the admin token)", cl.method, cl.path, cl.user, want)
		}
	}
	if !strings.Contains(calls[2].body, `"DRAIN"`) || !strings.Contains(calls[2].body, `"triage: disk"`) || !strings.Contains(calls[3].body, `"RESUME"`) {
		t.Errorf("update bodies: %q / %q", calls[2].body, calls[3].body)
	}

	// An error in the body (with HTTP 200) must surface.
	c.Admin = &read
	if err := c.Drain(ctx, "slinky-0", "x"); err == nil || !strings.Contains(err.Error(), "Invalid user id") {
		t.Errorf("want slurmrestd's error, got %v", err)
	}
	// No admin token: writes are unavailable, reads still work.
	c.Admin = nil
	if err := c.Resume(ctx, "slinky-0"); err == nil {
		t.Error("resume without an admin token must fail")
	}
}
