package slurm

import (
	"testing"
	"time"
)

func TestGresCount(t *testing.T) {
	tests := map[string]int{
		"":                        0,
		"(null)":                  0,
		"gpu:fake:4":              4,
		"gpu:fake:0(IDX:N/A)":     0,
		"gpu:fake:2(IDX:0-1)":     2,
		"gpu:a100:4,gpu:h100:4":   8,
		"gpu:8(S:0-1),nic:mlx5:2": 10,
	}
	for in, want := range tests {
		if got := GresCount(in); got != want {
			t.Errorf("GresCount(%q) = %d, want %d", in, got, want)
		}
	}
}

func TestParseEvents(t *testing.T) {
	loc := time.FixedZone("MDT", -6*3600)
	out := []byte(`|2026-09-24T20:45:24|Unknown||Cluster Registered TRES|
slurm-c2|2026-09-24T21:44:31|2026-09-24T21:44:39|DRAIN|NHC: check_fs_free|root(0)
slurm-c[1-2]|2026-09-24T21:50:00|Unknown|DOWN|Not responding|slurm(64030)
`)
	events, err := parseEvents(out, loc)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 3 {
		t.Fatalf("got %d events, want 3 (cluster event skipped, hostlist expanded): %+v", len(events), events)
	}
	if e := events[0]; e.Node != "slurm-c2" || e.State != "DRAIN" || e.End.IsZero() || !e.Start.Equal(time.Date(2026, 9, 24, 21, 44, 31, 0, loc)) {
		t.Errorf("closed drain event parsed wrong: %+v", e)
	}
	if e := events[2]; e.Node != "slurm-c2" || !e.End.IsZero() || e.Reason != "Not responding" {
		t.Errorf("open down event parsed wrong: %+v", e)
	}
}

func TestNumberTime(t *testing.T) {
	if !(Number{Set: true, Infinite: true, Number: 5}).Time().IsZero() {
		t.Error("infinite should have no time")
	}
	if got := (Number{Set: true, Number: 1790307891}).Time().Unix(); got != 1790307891 {
		t.Errorf("got %d", got)
	}
}
