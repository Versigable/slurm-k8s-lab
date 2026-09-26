package kube

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestWorkload(t *testing.T) {
	mk := func(phase, owner string, mirror bool) Pod {
		var p Pod
		p.Status.Phase = phase
		if owner != "" {
			p.Metadata.OwnerReferences = []OwnerReference{{Kind: owner}}
		}
		if mirror {
			p.Metadata.Annotations = map[string]string{"kubernetes.io/config.mirror": "x"}
		}
		return p
	}
	tests := []struct {
		name string
		pod  Pod
		want bool
	}{
		{"replicaset pod", mk("Running", "ReplicaSet", false), true},
		{"bare pod", mk("Pending", "", false), true},
		{"daemonset pod (drain skips it)", mk("Running", "DaemonSet", false), false},
		{"static pod (drain skips it)", mk("Running", "Node", true), false},
		{"finished pod", mk("Succeeded", "Job", false), false},
	}
	for _, tt := range tests {
		if got := tt.pod.Workload(); got != tt.want {
			t.Errorf("%s: Workload() = %v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestPDBCovers(t *testing.T) {
	var pdb PDB
	pdb.Metadata.Namespace = "app"
	pdb.Spec.Selector = &LabelSelector{MatchLabels: map[string]string{"app": "api"}}
	var p Pod
	p.Metadata.Namespace, p.Metadata.Labels = "app", map[string]string{"app": "api", "pod-template-hash": "x"}
	if !pdb.Covers(p) {
		t.Error("matching labels in the same namespace should be covered")
	}
	p.Metadata.Namespace = "other"
	if pdb.Covers(p) {
		t.Error("a PDB never covers pods in another namespace")
	}
}

// A real TLS server: checks the bearer token, CA trust, list decoding and the
// cordon merge patch.
func TestClientAgainstAPIServer(t *testing.T) {
	var gotPatch map[string]any
	var gotContentType string
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-token" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/nodes":
			io.WriteString(w, `{"items":[{"metadata":{"name":"k8s-w1"},"spec":{"unschedulable":true},
				"status":{"conditions":[{"type":"Ready","status":"True"}],"nodeInfo":{"kubeletVersion":"v1.36.5"}}}]}`)
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/events":
			if r.URL.Query().Get("fieldSelector") != "involvedObject.kind=Node" {
				http.Error(w, "want the node field selector", http.StatusBadRequest)
				return
			}
			io.WriteString(w, `{"items":[]}`)
		case r.Method == http.MethodPatch && r.URL.Path == "/api/v1/nodes/k8s-w1":
			gotContentType = r.Header.Get("Content-Type")
			json.NewDecoder(r.Body).Decode(&gotPatch)
			io.WriteString(w, `{}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	dir := t.TempDir()
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	for name, content := range map[string]string{"api-url": srv.URL + "\n", "token": "test-token\n", "ca.crt": string(caPEM)} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	c, err := NewFromDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	nodes, err := c.Nodes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 1 || !nodes[0].Spec.Unschedulable || nodes[0].Status.NodeInfo.KubeletVersion != "v1.36.5" {
		t.Fatalf("decoded nodes: %+v", nodes)
	}
	if _, err := c.NodeEvents(ctx); err != nil {
		t.Fatal(err)
	}

	if err := c.SetUnschedulable(ctx, "k8s-w1", true, "triage: disk errors"); err != nil {
		t.Fatal(err)
	}
	if gotContentType != "application/merge-patch+json" {
		t.Errorf("content type %q", gotContentType)
	}
	spec := gotPatch["spec"].(map[string]any)
	ann := gotPatch["metadata"].(map[string]any)["annotations"].(map[string]any)
	if spec["unschedulable"] != true || ann[ReasonAnnotation] != "triage: disk errors" {
		t.Errorf("cordon patch = %v", gotPatch)
	}

	// Uncordon clears the reason (null deletes the key in a merge patch).
	if err := c.SetUnschedulable(ctx, "k8s-w1", false, ""); err != nil {
		t.Fatal(err)
	}
	ann = gotPatch["metadata"].(map[string]any)["annotations"].(map[string]any)
	if v, present := ann[ReasonAnnotation]; !present || v != nil {
		t.Errorf("uncordon should null the annotation, got %v", gotPatch)
	}

	c.Token = "wrong"
	if _, err := c.Nodes(ctx); err == nil {
		t.Error("a rejected token must surface as an error")
	}
}
