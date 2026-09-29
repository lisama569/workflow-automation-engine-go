// Command wf is the local CLI for the workflow engine demo.
//
// Usage:
//
//	wf demo --data-dir=.wfdata --trigger=<id> [--mode=ok|retry|fail|timeout|parallel]
//	wf list --data-dir=.wfdata
//
// On startup it resumes all unfinished instances, demonstrating restart
// recovery and duplicate-delivery idempotency.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"time"

	"github.com/lisama569/workflow-automation-engine-go/workflow"
)

func main() {
	logger := log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds)
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cmd := os.Args[1]
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	dataDir := fs.String("data-dir", ".wfdata", "directory for state, ledger and counters")
	trigger := fs.String("trigger", "", "trigger id (delivery identifier)")
	mode := fs.String("mode", "ok", "demo mode: ok|retry|fail|timeout|parallel")
	_ = fs.Parse(os.Args[2:])

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	switch cmd {
	case "demo":
		runDemo(ctx, logger, *dataDir, *trigger, *mode)
	case "list":
		listInstances(ctx, *dataDir)
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: wf <demo|list> [flags]")
}

// demoDef builds the demo workflow definition for the chosen mode.
//
// The graph (ok/retry/fail/timeout modes):
//
//	validate -> [prepare | reserve] (parallel) -> charge -> receipt
//
// Parallel branches prepare and reserve have disjoint step states and never
// share mutable data. "parallel" mode runs four independent leaves to make
// concurrency visible in the logs.
func demoDef(mode string) workflow.Definition {
	const ms = 10
	base := func() workflow.Definition {
		return workflow.Definition{
			Name: "order-demo-" + mode,
			Steps: []workflow.StepDef{
				{Name: "validate", Activity: "noop", Input: map[string]any{"order": "A-1001"}},
				{Name: "prepare", Activity: activityFor(mode, "prepare"), DependsOn: []string{"validate"},
					Input: inputFor(mode, "prepare"), Timeout: 2 * time.Second, MaxAttempts: 4,
					InitialBackoff: 20 * time.Millisecond},
				{Name: "reserve", Activity: "noop", DependsOn: []string{"validate"},
					Input: map[string]any{"sku": "WIDGET", "qty": 2}},
				{Name: "charge", Activity: activityFor(mode, "charge"), DependsOn: []string{"prepare", "reserve"},
					Input: inputFor(mode, "charge"), Timeout: time.Second, MaxAttempts: 3,
					InitialBackoff: 20 * time.Millisecond},
				{Name: "receipt", Activity: "noop", DependsOn: []string{"charge"},
					Input: map[string]any{"channel": "email"}},
			},
		}
	}
	switch mode {
	case "parallel":
		steps := []workflow.StepDef{}
		for _, n := range []string{"p1", "p2", "p3", "p4"} {
			steps = append(steps, workflow.StepDef{
				Name: n, Activity: "slow", Input: map[string]any{"sleep_ms": ms * 3},
				Timeout: 2 * time.Second,
			})
		}
		return workflow.Definition{Name: "order-demo-parallel", Steps: steps}
	default:
		return base()
	}
}

func activityFor(mode, step string) string {
	switch {
	case mode == "retry" && step == "prepare":
		return "flaky"
	case mode == "fail" && step == "charge":
		return "alwaysfail"
	case mode == "timeout" && step == "charge":
		return "slow"
	default:
		return "noop"
	}
}

func inputFor(mode, step string) map[string]any {
	switch {
	case mode == "retry" && step == "prepare":
		return map[string]any{"fail_times": 2}
	case mode == "timeout" && step == "charge":
		return map[string]any{"sleep_ms": 1500}
	default:
		return map[string]any{}
	}
}

func runDemo(ctx context.Context, logger *log.Logger, dataDir, trigger, mode string) {
	if trigger == "" {
		trigger = "trg-" + time.Now().UTC().Format("20060102T150405.000000000")
		logger.Printf("no --trigger given, generated trigger id: %s", trigger)
	}
	store, err := workflow.NewFileStore(dataDir)
	if err != nil {
		logger.Fatalf("store: %v", err)
	}
	ledger, err := workflow.NewLedger(dataDir)
	if err != nil {
		logger.Fatalf("ledger: %v", err)
	}
	reg := workflow.NewRegistry()
	workflow.RegisterDemoActivities(reg, ledger, ledger)

	cfg := workflow.DefaultConfig()
	eng := workflow.NewEngine(store, reg, cfg, logger)

	def := demoDef(mode)

	// Restart recovery: finish anything a previous process left behind
	// before accepting this trigger.
	if err := eng.ResumeAll(ctx, def); err != nil {
		logger.Printf("resume: %v", err)
	}

	logger.Printf("triggering workflow name=%s trigger=%s mode=%s", def.Name, trigger, mode)
	res, err := eng.Run(ctx, def, trigger)
	if err != nil {
		logger.Printf("run returned error: %v", err)
	}
	printSummary(logger, res)
	if res != nil && res.State.Status == workflow.StatusFailed {
		os.Exit(1)
	}
}

func listInstances(ctx context.Context, dataDir string) {
	store, err := workflow.NewFileStore(dataDir)
	if err != nil {
		log.Fatalf("store: %v", err)
	}
	names := []string{
		"order-demo-ok", "order-demo-retry", "order-demo-fail",
		"order-demo-timeout", "order-demo-parallel",
	}
	for _, n := range names {
		list, err := store.List(ctx, n)
		if err != nil {
			log.Fatalf("list %s: %v", n, err)
		}
		for _, st := range list {
			fmt.Printf("%s\t%s\t%s\t(v%d)\n", st.WorkflowName, st.TriggerID, st.Status, st.Version)
		}
	}
}

func printSummary(logger *log.Logger, res *workflow.RunResult) {
	if res == nil {
		return
	}
	st := res.State
	logger.Printf("----- summary -----")
	logger.Printf("workflow=%s trigger=%s conclusion=%s adopted=%v version=%d err=%q",
		st.WorkflowName, st.TriggerID, st.Status, res.Adopted, st.Version, st.Err)
	for _, step := range orderedSteps(st) {
		last := "-"
		if n := len(step.Attempts); n > 0 {
			last = fmt.Sprintf("%d", n)
		}
		logger.Printf("  step=%-8s status=%-9s attempts=%s", step.Name, step.Status, last)
	}
}

// orderedSteps returns step states in definition-independent deterministic
// (alphabetical) order for summary printing.
func orderedSteps(st *workflow.WorkflowState) []*workflow.StepState {
	names := make([]string, 0, len(st.Steps))
	for n := range st.Steps {
		names = append(names, n)
	}
	sort.Strings(names)
	out := make([]*workflow.StepState, 0, len(names))
	for _, n := range names {
		out = append(out, st.Steps[n])
	}
	return out
}
