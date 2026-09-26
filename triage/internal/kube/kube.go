// Package kube reads the Kubernetes state node triage needs (nodes, pods,
// node events, PodDisruptionBudgets) straight from the API server, and makes
// the one change triage is allowed: cordon/uncordon.
//
// It speaks plain REST with a ServiceAccount bearer token instead of pulling
// in client-go: triage needs five calls, and the types below keep only the
// fields the rules read.
package kube

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"
)

// ReasonAnnotation records why triage (or an operator using its convention)
// cordoned a node. Kubernetes has no native cordon reason.
const ReasonAnnotation = "slurm-k8s-lab/triage-reason"

// Source is what triage reads from, and the one write it may make.
type Source interface {
	Nodes(ctx context.Context) ([]Node, error)
	Pods(ctx context.Context) ([]Pod, error)
	NodeEvents(ctx context.Context) ([]Event, error)
	PDBs(ctx context.Context) ([]PDB, error)
	// SetUnschedulable cordons (true, with a reason) or uncordons (false) a node.
	SetUnschedulable(ctx context.Context, node string, unschedulable bool, reason string) error
}

// --- types ---------------------------------------------------------------------------

type ObjectMeta struct {
	Name            string            `json:"name"`
	Namespace       string            `json:"namespace,omitempty"`
	Labels          map[string]string `json:"labels,omitempty"`
	Annotations     map[string]string `json:"annotations,omitempty"`
	OwnerReferences []OwnerReference  `json:"ownerReferences,omitempty"`
}

type OwnerReference struct {
	Kind string `json:"kind"`
	Name string `json:"name"`
}

type Node struct {
	Metadata ObjectMeta `json:"metadata"`
	Spec     struct {
		Unschedulable bool    `json:"unschedulable,omitempty"`
		Taints        []Taint `json:"taints,omitempty"`
	} `json:"spec"`
	Status struct {
		Conditions []Condition `json:"conditions"`
		NodeInfo   struct {
			KubeletVersion string `json:"kubeletVersion"`
		} `json:"nodeInfo"`
	} `json:"status"`
}

type Taint struct {
	Key       string     `json:"key"`
	Value     string     `json:"value,omitempty"`
	Effect    string     `json:"effect"`
	TimeAdded *time.Time `json:"timeAdded,omitempty"`
}

type Condition struct {
	Type               string    `json:"type"`
	Status             string    `json:"status"` // True, False, Unknown
	Reason             string    `json:"reason,omitempty"`
	Message            string    `json:"message,omitempty"`
	LastHeartbeatTime  time.Time `json:"lastHeartbeatTime"`
	LastTransitionTime time.Time `json:"lastTransitionTime"`
}

// Condition returns the node condition of the given type.
func (n Node) Condition(t string) (Condition, bool) {
	i := slices.IndexFunc(n.Status.Conditions, func(c Condition) bool { return c.Type == t })
	if i < 0 {
		return Condition{}, false
	}
	return n.Status.Conditions[i], true
}

// Taint returns the node's taint with the given key and effect.
func (n Node) Taint(key, effect string) (Taint, bool) {
	i := slices.IndexFunc(n.Spec.Taints, func(t Taint) bool { return t.Key == key && t.Effect == effect })
	if i < 0 {
		return Taint{}, false
	}
	return n.Spec.Taints[i], true
}

type Pod struct {
	Metadata ObjectMeta `json:"metadata"`
	Spec     struct {
		NodeName string `json:"nodeName"`
	} `json:"spec"`
	Status struct {
		Phase string `json:"phase"`
	} `json:"status"`
}

// Workload reports whether a drain would have to evict this pod: it's still
// running, and it isn't a DaemonSet pod or a static (mirror) pod, which drain skips.
func (p Pod) Workload() bool {
	if p.Status.Phase == "Succeeded" || p.Status.Phase == "Failed" {
		return false
	}
	if _, mirror := p.Metadata.Annotations["kubernetes.io/config.mirror"]; mirror {
		return false
	}
	return !slices.ContainsFunc(p.Metadata.OwnerReferences, func(o OwnerReference) bool { return o.Kind == "DaemonSet" })
}

func (p Pod) Ref() string { return p.Metadata.Namespace + "/" + p.Metadata.Name }

type Event struct {
	Metadata       ObjectMeta `json:"metadata"`
	InvolvedObject struct {
		Kind string `json:"kind"`
		Name string `json:"name"`
	} `json:"involvedObject"`
	Reason  string `json:"reason"`
	Message string `json:"message"`
	Type    string `json:"type"` // Normal, Warning
	Count   int    `json:"count"`
	Source  struct {
		Component string `json:"component"`
		Host      string `json:"host"`
	} `json:"source"`
	FirstTimestamp time.Time  `json:"firstTimestamp"`
	LastTimestamp  time.Time  `json:"lastTimestamp"`
	EventTime      *time.Time `json:"eventTime,omitempty"`
}

// When is the most recent time the event was seen.
func (e Event) When() time.Time {
	if !e.LastTimestamp.IsZero() {
		return e.LastTimestamp
	}
	if e.EventTime != nil {
		return *e.EventTime
	}
	return e.FirstTimestamp
}

// Occurrences is the event's repeat count (at least 1).
func (e Event) Occurrences() int { return max(e.Count, 1) }

type PDB struct {
	Metadata ObjectMeta `json:"metadata"`
	Spec     struct {
		Selector *LabelSelector `json:"selector"`
	} `json:"spec"`
	Status struct {
		DisruptionsAllowed int `json:"disruptionsAllowed"`
		CurrentHealthy     int `json:"currentHealthy"`
		DesiredHealthy     int `json:"desiredHealthy"`
		ExpectedPods       int `json:"expectedPods"`
	} `json:"status"`
}

type LabelSelector struct {
	MatchLabels      map[string]string `json:"matchLabels,omitempty"`
	MatchExpressions []json.RawMessage `json:"matchExpressions,omitempty"`
}

// Covers reports whether the PDB selects pod. Only matchLabels are evaluated;
// a selector with matchExpressions is treated as covering nothing, so triage
// never claims a PDB blocks a drain unless it can show it.
func (p PDB) Covers(pod Pod) bool {
	sel := p.Spec.Selector
	if sel == nil || pod.Metadata.Namespace != p.Metadata.Namespace || len(sel.MatchExpressions) > 0 || len(sel.MatchLabels) == 0 {
		return false
	}
	for k, v := range sel.MatchLabels {
		if pod.Metadata.Labels[k] != v {
			return false
		}
	}
	return true
}

func (p PDB) Ref() string { return p.Metadata.Namespace + "/" + p.Metadata.Name }

// --- API client ------------------------------------------------------------------------------

// Client talks to the API server with a bearer token.
type Client struct {
	BaseURL string
	Token   string
	HTTP    *http.Client
}

// NewFromDir builds a client from api-url, token and ca.crt in dir
// (written to /etc/node-triage by ansible/triage.yml).
func NewFromDir(dir string) (*Client, error) {
	read := func(name string) ([]byte, error) { return os.ReadFile(filepath.Join(dir, name)) }
	apiURL, err := read("api-url")
	if err != nil {
		return nil, err
	}
	token, err := read("token")
	if err != nil {
		return nil, err
	}
	caPEM, err := read("ca.crt")
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("%s/ca.crt: no certificates", dir)
	}
	return &Client{
		BaseURL: strings.TrimRight(strings.TrimSpace(string(apiURL)), "/"),
		Token:   strings.TrimSpace(string(token)),
		HTTP: &http.Client{
			Timeout:   15 * time.Second,
			Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}},
		},
	}, nil
}

type list[T any] struct {
	Items []T `json:"items"`
}

func (c *Client) do(ctx context.Context, method, path, contentType string, body []byte, v any) error {
	req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
	req.Header.Set("Accept", "application/json")
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
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
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("%s %s: %s: %s", method, path, resp.Status, strings.TrimSpace(string(data)))
	}
	if v == nil {
		return nil
	}
	return json.Unmarshal(data, v)
}

func getList[T any](ctx context.Context, c *Client, path string) ([]T, error) {
	var l list[T]
	if err := c.do(ctx, http.MethodGet, path, "", nil, &l); err != nil {
		return nil, err
	}
	return l.Items, nil
}

func (c *Client) Nodes(ctx context.Context) ([]Node, error) {
	return getList[Node](ctx, c, "/api/v1/nodes")
}

func (c *Client) Pods(ctx context.Context) ([]Pod, error) {
	return getList[Pod](ctx, c, "/api/v1/pods")
}

func (c *Client) NodeEvents(ctx context.Context) ([]Event, error) {
	return getList[Event](ctx, c, "/api/v1/events?fieldSelector="+url.QueryEscape("involvedObject.kind=Node"))
}

func (c *Client) PDBs(ctx context.Context) ([]PDB, error) {
	return getList[PDB](ctx, c, "/apis/policy/v1/poddisruptionbudgets")
}

func (c *Client) SetUnschedulable(ctx context.Context, node string, unschedulable bool, reason string) error {
	return c.do(ctx, http.MethodPatch, "/api/v1/nodes/"+url.PathEscape(node),
		"application/merge-patch+json", cordonPatch(unschedulable, reason), nil)
}

// cordonPatch is a JSON merge patch; a null annotation value deletes it.
func cordonPatch(unschedulable bool, reason string) []byte {
	var ann any
	if unschedulable {
		ann = reason
	}
	b, _ := json.Marshal(map[string]any{
		"spec":     map[string]any{"unschedulable": unschedulable},
		"metadata": map[string]any{"annotations": map[string]any{ReasonAnnotation: ann}},
	})
	return b
}

// --- fixtures ------------------------------------------------------------------------------

// FixtureSource serves a directory captured by scripts/capture-k8s-scenarios.sh.
// Writes are recorded, not executed.
type FixtureSource struct {
	Dir string

	mu     sync.Mutex
	writes []string
}

func readFixture[T any](dir, file string) ([]T, error) {
	data, err := os.ReadFile(filepath.Join(dir, file))
	if err != nil {
		return nil, err
	}
	var l list[T]
	if err := json.Unmarshal(data, &l); err != nil {
		return nil, fmt.Errorf("%s: %w", file, err)
	}
	return l.Items, nil
}

func (f *FixtureSource) Nodes(context.Context) ([]Node, error) {
	return readFixture[Node](f.Dir, "nodes.json")
}

func (f *FixtureSource) Pods(context.Context) ([]Pod, error) {
	return readFixture[Pod](f.Dir, "pods.json")
}

func (f *FixtureSource) NodeEvents(context.Context) ([]Event, error) {
	return readFixture[Event](f.Dir, "events.json")
}

func (f *FixtureSource) PDBs(context.Context) ([]PDB, error) {
	return readFixture[PDB](f.Dir, "pdbs.json")
}

func (f *FixtureSource) SetUnschedulable(_ context.Context, node string, unschedulable bool, reason string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.writes = append(f.writes, fmt.Sprintf("PATCH /api/v1/nodes/%s %s", node, cordonPatch(unschedulable, reason)))
	return nil
}

// Writes returns the patches that would have been sent.
func (f *FixtureSource) Writes() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.writes...)
}
