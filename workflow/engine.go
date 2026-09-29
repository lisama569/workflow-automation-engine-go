package workflow

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)

// Failure convention (kept identical for serial and parallel steps):
//
//   - A step whose activity returns a non-retryable error, or whose retries
//     are exhausted, or whose per-attempt timeout fires for every attempt,
//     enters StatusFailed: a TERMINAL state.
//   - All steps that had not yet run are moved to StatusSkipped (terminal).
//   - Steps already Succeeded keep their result; they are never rolled back
//     or re-executed.
//   - Steps of the same parallel wave that are still in flight when another
//     step fails have their context canceled. Their attempt is recorded
//     (canceled/timed-out as appropriate) and they follow the retry rules;
//     if the workflow cannot continue they end Failed/Skipped like the rest.
//
// In short: fail fast, one Failed step fails the workflow, completed work is
// preserved and never repeated.

// Config holds engine defaults.
type Config struct {
	// DefaultMaxAttempts is used when StepDef.MaxAttempts is zero.
	DefaultMaxAttempts int
	// DefaultTimeout bounds a single attempt when the step does not set one.
	DefaultTimeout time.Duration
	// DefaultInitialBackoff is the base retry delay (doubled each retry).
	DefaultInitialBackoff time.Duration
	// MaxBackoff caps the exponential backoff.
	MaxBackoff time.Duration
}

// DefaultConfig returns sensible local defaults.
func DefaultConfig() Config {
	return Config{
		DefaultMaxAttempts:    3,
		DefaultTimeout:        5 * time.Second,
		DefaultInitialBackoff: 50 * time.Millisecond,
		MaxBackoff:            2 * time.Second,
	}
}

// errForeignExecution signals that a CAS conflict proved another process is
// already advancing this instance, so the current caller must not execute it.
var errForeignExecution = errors.New("workflow: instance is executed by another process")

// Engine executes definitions against a store and an activity registry.
type Engine struct {
	cfg      Config
	store    Store
	registry *Registry
	logger   *log.Logger

	execMu sync.Mutex
	exec   map[string]chan struct{} // trigger keys currently executing in-process
}

// NewEngine constructs an engine.
func NewEngine(store Store, registry *Registry, cfg Config, logger *log.Logger) *Engine {
	if logger == nil {
		logger = log.Default()
	}
	e := &Engine{cfg: cfg, store: store, registry: registry, logger: logger, exec: map[string]chan struct{}{}}
	return e
}

func execKey(workflowName, triggerID string) string { return workflowName + "/" + triggerID }

// acquireExec ensures only one goroutine per trigger executes in this process.
// It returns a release function and whether the caller waited for an earlier
// run. When several duplicate deliveries are queued, only one at a time takes
// over; the rest keep waiting and ultimately observe the terminal state.
func (e *Engine) acquireExec(ctx context.Context, workflowName, triggerID string) (release func(), waited bool, err error) {
	key := execKey(workflowName, triggerID)
	e.execMu.Lock()
	for {
		ch, busy := e.exec[key]
		if !busy {
			ch := make(chan struct{})
			e.exec[key] = ch
			e.execMu.Unlock()
			return func() {
				e.execMu.Lock()
				if cur, ok := e.exec[key]; ok && cur == ch {
					delete(e.exec, key)
				}
				e.execMu.Unlock()
				close(ch)
			}, waited, nil
		}
		e.execMu.Unlock()
		select {
		case <-ch:
			waited = true
			e.execMu.Lock()
		case <-ctx.Done():
			return func() {}, waited, ctx.Err()
		}
	}
}

func (e *Engine) logf(format string, args ...any) {
	e.logger.Output(2, fmt.Sprintf(format, args...))
}

// Run triggers a workflow.
//
//   - First triggerID: a new instance is created and executed to a terminal
//     state (or until ctx is canceled; call Run again / restart to resume).
//   - Duplicate triggerID (same workflow name): the existing instance is
//     adopted; if unfinished it is resumed, otherwise its terminal state is
//     returned. Exactly one instance ever exists per triggerID.
func (e *Engine) Run(ctx context.Context, def Definition, triggerID string) (*RunResult, error) {
	if err := validateDef(def); err != nil {
		return nil, err
	}

	// Serialize same-trigger executions within this process. Concurrent and
	// duplicate deliveries block here and share the single execution below;
	// when it finishes they read the same terminal state.
	release, waited, err := e.acquireExec(ctx, def.Name, triggerID)
	if err != nil {
		return nil, err
	}
	defer release()

	initial := newState(def, triggerID)
	st, adopted, err := e.store.CreateIfAbsent(ctx, initial)
	if err != nil {
		return nil, fmt.Errorf("create instance: %w", err)
	}
	_ = adopted
	if adopted || waited {
		e.logf("[trigger=%s] duplicate delivery detected (in-process wait=%v, status=%s, version=%d)",
			triggerID, waited, st.Status, st.Version)
	} else {
		e.logf("[trigger=%s] new workflow instance created: %s", triggerID, def.Name)
	}
	if st.Status.Terminal() {
		return &RunResult{State: st, Adopted: adopted || waited}, nil
	}

	// The instance is unfinished. Another OS process may already be executing
	// it: try the loop; execute() returns errForeignExecution on the first CAS
	// conflict, meaning someone else owns progress. In that case — and only in
	// that case — wait for their terminal state instead of running anything.
	execErr := e.execute(ctx, def, st)
	if execErr != nil && !errors.Is(execErr, errForeignExecution) {
		cur, _ := e.store.Get(ctx, def.Name, triggerID)
		if cur != nil {
			st = cur
		}
		return &RunResult{State: st, Adopted: adopted || waited}, execErr
	}

	final, err := e.waitTerminal(ctx, def.Name, triggerID)
	if err != nil {
		return nil, err
	}
	e.logf("[trigger=%s] final conclusion: %s", triggerID, final.Status)
	return &RunResult{State: final, Adopted: adopted || waited}, nil
}

// waitTerminal polls the store until the instance reaches a terminal state.
// The poll interval is intentionally short for a local demo engine.
func (e *Engine) waitTerminal(ctx context.Context, workflowName, triggerID string) (*WorkflowState, error) {
	const interval = 20 * time.Millisecond
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		st, err := e.store.Get(ctx, workflowName, triggerID)
		if err != nil {
			return nil, err
		}
		if st.Status.Terminal() {
			return st, nil
		}
		select {
		case <-ctx.Done():
			return st, ctx.Err()
		case <-ticker.C:
		}
	}
}

// ResumeAll resumes every non-terminal instance of def in the store.
// Intended to be called once at process startup.
func (e *Engine) ResumeAll(ctx context.Context, def Definition) error {
	if err := validateDef(def); err != nil {
		return err
	}
	list, err := e.store.List(ctx, def.Name)
	if err != nil {
		return err
	}
	for _, st := range list {
		if st.Status.Terminal() {
			continue
		}
		release, _, lerr := e.acquireExec(ctx, def.Name, st.TriggerID)
		if lerr != nil {
			return lerr
		}
		// Re-read under the exec lock: an earlier resume may have finished it.
		fresh, gerr := e.store.Get(ctx, def.Name, st.TriggerID)
		if gerr != nil || fresh.Status.Terminal() {
			release()
			continue
		}
		e.logf("[trigger=%s] resuming unfinished instance after startup (status=%s)", st.TriggerID, fresh.Status)
		if err := e.execute(ctx, def, fresh); err != nil && !errors.Is(err, errForeignExecution) {
			e.logf("[trigger=%s] resume interrupted: %v", st.TriggerID, err)
		}
		release()
	}
	return nil
}

// Get returns the current state of an instance.
func (e *Engine) Get(ctx context.Context, workflowName, triggerID string) (*WorkflowState, error) {
	return e.store.Get(ctx, workflowName, triggerID)
}

// ---------------------------------------------------------------------------// state construction & validation
// ---------------------------------------------------------------------------

func newState(def Definition, triggerID string) *WorkflowState {
	now := nowTS()
	st := &WorkflowState{
		WorkflowName: def.Name,
		TriggerID:    triggerID,
		Status:       StatusPending,
		Steps:        make(map[string]*StepState, len(def.Steps)),
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	for _, s := range def.Steps {
		st.Steps[s.Name] = &StepState{Name: s.Name, Activity: s.Activity, Status: StatusPending}
	}
	return st
}

func validateDef(def Definition) error {
	if def.Name == "" {
		return errors.New("workflow: empty workflow name")
	}
	seen := map[string]bool{}
	for _, s := range def.Steps {
		if s.Name == "" {
			return errors.New("workflow: step with empty name")
		}
		if seen[s.Name] {
			return fmt.Errorf("workflow: duplicate step name %q", s.Name)
		}
		seen[s.Name] = true
	}
	for _, s := range def.Steps {
		for _, d := range s.DependsOn {
			if !seen[d] {
				return fmt.Errorf("workflow: step %q depends on unknown step %q", s.Name, d)
			}
		}
	}
	// Reject dependency cycles with a simple DFS.
	color := map[string]int{} // 0 white, 1 gray, 2 black
	var visit func(name string) error
	visit = func(name string) error {
		switch color[name] {
		case 1:
			return fmt.Errorf("workflow: dependency cycle at step %q", name)
		case 2:
			return nil
		}
		color[name] = 1
		for _, s := range def.Steps {
			if s.Name == name {
				for _, d := range s.DependsOn {
					if err := visit(d); err != nil {
						return err
					}
				}
			}
		}
		color[name] = 2
		return nil
	}
	for _, s := range def.Steps {
		if err := visit(s.Name); err != nil {
			return err
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// execution loop
// ---------------------------------------------------------------------------

// attemptOutcome is the result of running one step's current attempt wave.
type attemptOutcome struct {
	name      string
	step      *StepDef
	attempt   Attempt
	retriable bool
}

func (e *Engine) execute(ctx context.Context, def Definition, start *WorkflowState) error {
	byName := map[string]StepDef{}
	for _, s := range def.Steps {
		byName[s.Name] = s
	}

	// Any step still marked Running is a crash/orphan survivor: its prior
	// attempt has an unknown outcome, so we append a fresh attempt. Steps are
	// never marked Succeeded without a persisted successful attempt, which is
	// why no completed step is ever repeated.
	st := cloneState(start)
	for _, ss := range st.Steps {
		if ss.Status == StatusRunning && len(ss.Attempts) > 0 {
			e.logf("[trigger=%s] step=%s recovering orphaned running attempt", st.TriggerID, ss.Name)
		}
	}

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if st.Status.Terminal() {
			return nil
		}

		// 1. Decide transitions without executing anything.
		var wave []StepDef
		var skipNames []string
		budget := 0
		for _, sd := range def.Steps {
			ss := st.Steps[sd.Name]
			switch ss.Status {
			case StatusSucceeded, StatusSkipped, StatusFailed:
				continue
			case StatusRunning:
				// Resume path: retry the in-flight step as a new attempt.
				wave = append(wave, sd)
				budget += e.maxAttempts(sd) - len(ss.Attempts)
			case StatusPending:
				deps := sd.DependsOn
				blocked, failed := false, false
				for _, d := range deps {
					switch st.Steps[d].Status {
					case StatusFailed, StatusSkipped:
						failed = true
					case StatusSucceeded:
					default:
						blocked = true
					}
				}
				if failed {
					skipNames = append(skipNames, sd.Name)
				} else if !blocked {
					wave = append(wave, sd)
				}
			}
		}

		// 2. Persist "running"/"skipped" decisions before side effects so a
		//    crash cannot lose or duplicate the bookkeeping.
		if len(skipNames) > 0 {
			for _, n := range skipNames {
				st.Steps[n].Status = StatusSkipped
				st.Steps[n].EndedAt = nowTS()
				e.logf("[trigger=%s] step=%s -> skipped (upstream failure)", st.TriggerID, n)
			}
		}
		for i := range wave {
			sd := wave[i]
			ss := st.Steps[sd.Name]
			if ss.Status != StatusRunning {
				ss.Status = StatusRunning
				if ss.StartedAt == "" {
					ss.StartedAt = nowTS()
				}
				e.logf("[trigger=%s] step=%s -> running (parallel wave, deps=%v)", st.TriggerID, sd.Name, sd.DependsOn)
			}
		}
		st.UpdatedAt = nowTS()
		st.Status = aggregateStatus(st)
		var err error
		st, err = e.swapWithRetry(ctx, st)
		if err != nil {
			return err
		}

		// Nothing runnable and nothing pending: we are done.
		if len(wave) == 0 {
			if !st.Status.Terminal() {
				// Defensive: all steps terminal but aggregate not.
				st.Status = aggregateStatus(st)
				st, err = e.swapWithRetry(ctx, st)
				if err != nil {
					return err
				}
			}
			if st.Status.Terminal() {
				return nil
			}
			return fmt.Errorf("workflow: no runnable step but instance not terminal")
		}

		// 3. Run the wave concurrently. A terminal failure in any step
		//    cancels the contexts of the others (fail-fast convention).
		waveCtx, cancel := context.WithCancel(ctx)
		outcomes := make([]attemptOutcome, len(wave))
		done := make(chan struct{}, len(wave))
		for i := range wave {
			sd := wave[i]
			go func(i int) {
				defer func() { done <- struct{}{} }()
				outcomes[i] = e.runStep(waveCtx, st.WorkflowName, st.TriggerID, sd, st.Steps[sd.Name])
				if outcomes[i].attempt.Err != "" && !outcomes[i].retriable {
					cancel() // terminal failure: stop the rest of this wave
				}
			}(i)
		}
		for range wave {
			<-done
		}
		cancel()

		// 4. Apply outcomes to local state. Retryable failures stay Running
		//    and are revisited by the next loop tick with backoff.
		var retrySleep time.Duration
		terminalFailure := false
		for _, oc := range outcomes {
			ss := st.Steps[oc.name]
			ss.Attempts = append(ss.Attempts, oc.attempt)
			switch {
			case oc.attempt.Err == "":
				ss.Status = StatusSucceeded
				ss.EndedAt = oc.attempt.EndedAt
				e.logf("[trigger=%s] step=%s -> succeeded on attempt %d output=%v",
					st.TriggerID, oc.name, oc.attempt.AttemptNo, oc.attempt.Output)
			case !oc.retriable:
				ss.Status = StatusFailed
				ss.EndedAt = oc.attempt.EndedAt
				terminalFailure = true
				e.logf("[trigger=%s] step=%s -> FAILED (non-retryable) on attempt %d: %s",
					st.TriggerID, oc.name, oc.attempt.AttemptNo, oc.attempt.Err)
			case len(ss.Attempts) >= e.maxAttempts(*oc.step):
				ss.Status = StatusFailed
				ss.EndedAt = oc.attempt.EndedAt
				terminalFailure = true
				e.logf("[trigger=%s] step=%s -> FAILED after %d attempts (retry budget exhausted): %s",
					st.TriggerID, oc.name, len(ss.Attempts), oc.attempt.Err)
			default:
				// remains Running; will be retried next tick
				d := e.backoff(*oc.step, len(ss.Attempts))
				if d > retrySleep {
					retrySleep = d
				}
				e.logf("[trigger=%s] step=%s attempt %d failed (retryable): %s — will retry in %s",
					st.TriggerID, oc.name, oc.attempt.AttemptNo, oc.attempt.Err, d)
			}
		}
		if terminalFailure {
			// Fail-fast convention: the failing step(s) stay Failed; every
			// other unfinished step (pending dependency or wave sibling
			// canceled in flight) becomes Skipped. Succeeded steps are kept.
			for _, ss0 := range st.Steps {
				if ss0.Status == StatusPending || ss0.Status == StatusRunning {
					ss0.Status = StatusSkipped
					ss0.EndedAt = nowTS()
				}
			}
			st.Err = fmt.Sprintf("step %q failed terminally", firstFailedStep(st))
			e.logf("[trigger=%s] fail-fast: remaining unfinished steps skipped", st.TriggerID)
		}

		// 5. Persist the wave results before any sleep / exit.
		st.UpdatedAt = nowTS()
		st.Status = aggregateStatus(st)
		st, err = e.swapWithRetry(ctx, st)
		if err != nil {
			return err
		}
		if st.Status.Terminal() {
			if st.Status == StatusSucceeded {
				e.logf("[trigger=%s] all %d steps succeeded", st.TriggerID, len(def.Steps))
			} else {
				e.logf("[trigger=%s] workflow FAILED: %s", st.TriggerID, st.Err)
			}
			return nil
		}

		// 6. Backoff between retry waves; interruptible by ctx cancellation.
		if retrySleep > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(retrySleep):
			}
		}
		_ = budget
	}
}

// runStep executes exactly one attempt of a step, enforcing per-attempt
// timeout. Timeouts are treated as retryable; explicitly non-retryable
// activity errors are not.
func (e *Engine) runStep(ctx context.Context, workflowName, triggerID string, sd StepDef, ss *StepState) attemptOutcome {
	attemptNo := len(ss.Attempts) + 1
	fn, err := e.registry.lookup(sd.Activity)
	started := nowTS()
	ac := ActivityContext{
		WorkflowName: workflowName,
		TriggerID:    triggerID,
		StepName:     sd.Name,
		Attempt:      attemptNo,
	}
	if err != nil {
		return attemptOutcome{
			name: sd.Name, step: &sd,
			attempt: Attempt{
				AttemptNo: attemptNo, StartedAt: started, EndedAt: nowTS(),
				Input: sd.Input, Err: err.Error(), Retryable: false,
			},
		}
	}

	timeout := e.timeout(sd)
	attemptCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	type res struct {
		out map[string]any
		err error
	}
	ch := make(chan res, 1)
	go func() {
		out, aerr := fn(attemptCtx, ac, cloneInput(sd.Input))
		ch <- res{out, aerr}
	}()

	var (
		out      map[string]any
		aerr     error
		timedOut bool
		canceled bool
	)
	select {
	case r := <-ch:
		out, aerr = r.out, r.err
	case <-attemptCtx.Done():
		if errors.Is(attemptCtx.Err(), context.DeadlineExceeded) {
			timedOut = true
			aerr = fmt.Errorf("step %s timed out after %s", sd.Name, timeout)
		} else {
			canceled = true
			aerr = fmt.Errorf("step %s canceled: %w", sd.Name, attemptCtx.Err())
		}
		// Wait for the handler to notice cancellation (best effort; we do
		// not block shutdown on an unresponsive activity).
		select {
		case <-ch:
		case <-time.After(200 * time.Millisecond):
		}
	}

	att := Attempt{
		AttemptNo: attemptNo,
		StartedAt: started,
		EndedAt:   nowTS(),
		Input:     cloneInput(sd.Input),
	}
	retryable := false
	if aerr != nil {
		att.Err = aerr.Error()
		switch {
		case timedOut:
			retryable = true // timeouts are transient
			att.TimedOut = true
		case canceled:
			retryable = true // wave cancel due to a sibling failure; harmless to mark retryable
		default:
			retryable = IsRetryable(aerr)
		}
		att.Retryable = retryable
	} else {
		att.Output = out
	}
	return attemptOutcome{name: sd.Name, step: &sd, attempt: att, retriable: aerr != nil && retryable}
}

// swapWithRetry persists st with optimistic CAS. On conflict it reloads,
// adopts the persisted version, and reports it back; the next scheduler tick
// reconciles (this is how duplicate runners converge without losing state).
func (e *Engine) swapWithRetry(ctx context.Context, st *WorkflowState) (*WorkflowState, error) {
	const maxTries = 10
	want := st.Version
	for tries := 0; ; tries++ {
		err := e.store.CompareAndSwap(ctx, st, want)
		if err == nil {
			saved := cloneState(st)
			saved.Version = want + 1
			return saved, nil
		}
		if !errors.Is(err, ErrConflict) {
			return nil, err
		}
		fresh, gerr := e.store.Get(ctx, st.WorkflowName, st.TriggerID)
		if gerr != nil {
			return nil, gerr
		}
		e.logf("[trigger=%s] CAS conflict (v%d vs v%d): another runner is advancing this instance",
			st.TriggerID, want, fresh.Version)
		return nil, errForeignExecution
	}
}

// aggregateStatus computes workflow status from step statuses.
//
// Failed  -> workflow Failed; all steps Skipped/Succeeded -> Succeeded;
// any non-terminal step -> Running (or Pending before first launch).
func aggregateStatus(st *WorkflowState) Status {
	pending, running := 0, 0
	for _, ss := range st.Steps {
		switch ss.Status {
		case StatusFailed:
			return StatusFailed
		case StatusPending:
			pending++
		case StatusRunning:
			running++
		}
	}
	if pending == 0 && running == 0 {
		return StatusSucceeded
	}
	if running > 0 {
		return StatusRunning
	}
	return StatusPending
}

func firstFailedStep(st *WorkflowState) string {
	for _, ss := range st.Steps {
		if ss.Status == StatusFailed {
			return ss.Name
		}
	}
	return ""
}

func (e *Engine) maxAttempts(s StepDef) int {
	if s.MaxAttempts > 0 {
		return s.MaxAttempts
	}
	return e.cfg.DefaultMaxAttempts
}

func (e *Engine) timeout(s StepDef) time.Duration {
	if s.Timeout > 0 {
		return s.Timeout
	}
	return e.cfg.DefaultTimeout
}

func (e *Engine) backoff(s StepDef, failedAttempts int) time.Duration {
	base := e.cfg.DefaultInitialBackoff
	if s.InitialBackoff > 0 {
		base = s.InitialBackoff
	}
	d := base
	for i := 1; i < failedAttempts; i++ {
		d *= 2
		if d >= e.cfg.MaxBackoff {
			return e.cfg.MaxBackoff
		}
	}
	if d > e.cfg.MaxBackoff {
		d = e.cfg.MaxBackoff
	}
	return d
}

func cloneInput(in map[string]any) map[string]any {
	if in == nil {
		return map[string]any{}
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
