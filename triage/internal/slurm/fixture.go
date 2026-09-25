package slurm

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// FixtureRunner answers Slurm commands from a directory captured by
// scripts/capture-scenarios.sh, so triage can run with no cluster (tests, demos).
// Write commands are recorded, not executed.
type FixtureRunner struct {
	Dir string

	mu     sync.Mutex
	writes []string
}

var fixtureFiles = map[string]string{
	"scontrol": "nodes.json",
	"squeue":   "squeue.json",
	"sacct":    "sacct.json",
	"sacctmgr": "events.txt",
}

func (f *FixtureRunner) Run(_ context.Context, name string, args ...string) ([]byte, error) {
	if name == "scontrol" && len(args) > 0 && args[0] == "update" {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.writes = append(f.writes, name+" "+strings.Join(args, " "))
		return nil, nil
	}
	file, ok := fixtureFiles[name]
	if !ok {
		return nil, fmt.Errorf("fixture runner: no fixture for %q", name)
	}
	return os.ReadFile(filepath.Join(f.Dir, file))
}

// Writes returns the update commands that would have been executed.
func (f *FixtureRunner) Writes() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.writes...)
}

// CapturedAt returns when the fixture was captured (now.txt, Unix seconds).
// Triage evaluates the scenario as of this instant so results are reproducible.
func (f *FixtureRunner) CapturedAt() (time.Time, error) {
	b, err := os.ReadFile(filepath.Join(f.Dir, "now.txt"))
	if err != nil {
		return time.Time{}, err
	}
	sec, err := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("now.txt: %w", err)
	}
	return time.Unix(sec, 0), nil
}
