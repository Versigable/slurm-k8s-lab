// Command node-triage is an MCP server (stdio) that triages Slurm nodes.
//
// Run it where the Slurm CLIs work (controller or login node), usually over SSH
// from the MCP client:
//
//	ssh -T labadmin@slurm-ctl /usr/local/bin/node-triage
//
// Or offline against a captured scenario:
//
//	node-triage -fixtures internal/slurm/testdata/draining
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
	_ "time/tzdata" // sacctmgr timestamps are zone-less; -tz must resolve anywhere

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/Versigable/slurm-k8s-lab/triage/internal/server"
	"github.com/Versigable/slurm-k8s-lab/triage/internal/slurm"
	"github.com/Versigable/slurm-k8s-lab/triage/internal/triage"
)

var version = "dev" // set with -ldflags "-X main.version=..."

func main() {
	var (
		allowWrites = flag.Bool("allow-writes", false, "register drain_node and resume_node (off: the server can't change the cluster)")
		fixtures    = flag.String("fixtures", "", "serve a captured scenario directory instead of the live cluster")
		history     = flag.Duration("history", 24*time.Hour, "sacct window for NODE_FAIL evidence")
		events      = flag.Duration("events", 7*24*time.Hour, "sacctmgr event window for chronic-node detection")
		nodeFail    = flag.Int("node-fail-threshold", triage.DefaultPolicy.NodeFailDrainThreshold, "NODE_FAIL jobs before recommending a drain")
		chronic     = flag.Int("chronic-threshold", triage.DefaultPolicy.ChronicEventThreshold, "drain/down events that mark a node chronic")
		tz          = flag.String("tz", "", "time zone of sacctmgr timestamps (default: local)")
		showVersion = flag.Bool("version", false, "print version and exit")
	)
	flag.Parse()
	if *showVersion {
		fmt.Println(version)
		return
	}
	log.SetOutput(os.Stderr) // stdout carries the MCP protocol
	log.SetPrefix("node-triage: ")

	loc := time.Local
	if *tz != "" {
		var err error
		if loc, err = time.LoadLocation(*tz); err != nil {
			log.Fatal(err)
		}
	}

	cfg := server.Config{
		Policy:        triage.Policy{NodeFailDrainThreshold: *nodeFail, ChronicEventThreshold: *chronic},
		HistoryWindow: *history,
		EventWindow:   *events,
		AllowWrites:   *allowWrites,
		Version:       version,
	}
	if *fixtures != "" {
		fr := &slurm.FixtureRunner{Dir: *fixtures}
		at, err := fr.CapturedAt()
		if err != nil {
			log.Fatalf("fixtures: %v", err)
		}
		cfg.Client = &slurm.Client{Runner: fr, Location: loc}
		cfg.Now = func() time.Time { return at }
		log.Printf("serving fixtures from %s (captured %s); writes are recorded, not executed", *fixtures, at.Format(time.RFC3339))
	} else {
		cfg.Client = &slurm.Client{Runner: slurm.ExecRunner{}, Location: loc}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := server.New(cfg).Run(ctx, &mcp.StdioTransport{}); err != nil && ctx.Err() == nil {
		log.Fatal(err)
	}
}
