package workflow

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDefinitionJSONSample(t *testing.T) {
	// 仓库根目录下的 examples/order-pipeline.json
	p := filepath.Join("..", "..", "examples", "order-pipeline.json")
	b, err := os.ReadFile(p)
	if err != nil {
		t.Skipf("sample file unavailable: %v", err)
	}
	var def Definition
	if err := json.Unmarshal(b, &def); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if def.Name != "order-pipeline" || len(def.Steps) != 3 {
		t.Fatalf("unexpected definition: %+v", def)
	}
	fanout := def.Steps[1]
	if fanout.Kind != KindParallel || len(fanout.Children) != 2 {
		t.Fatalf("fanout not a parallel container with 2 branches: %+v", fanout)
	}
	reserve := fanout.Children[1]
	if reserve.Activity != "reserve_inventory" {
		t.Fatalf("unexpected activity: %s", reserve.Activity)
	}
	if reserve.Timeout.Std() != 5*time.Second {
		t.Errorf("timeout=%s want 5s", reserve.Timeout.Std())
	}
	if reserve.RetryPolicy == nil || reserve.RetryPolicy.MaxAttempts != 3 ||
		reserve.RetryPolicy.InitialBackoff.Std() != 20*time.Millisecond {
		t.Errorf("retry policy not parsed: %+v", reserve.RetryPolicy)
	}

	// 再序列化回来应当保持可读且等价。
	out, err := json.Marshal(def)
	if err != nil {
		t.Fatal(err)
	}
	var def2 Definition
	if err := json.Unmarshal(out, &def2); err != nil {
		t.Fatal(err)
	}
	if def2.Steps[1].Children[1].Timeout.Std() != 5*time.Second {
		t.Errorf("round-trip mismatch")
	}
}

func TestStepStatusTerminal(t *testing.T) {
	if StepPending.Terminal() || StepRunning.Terminal() {
		t.Errorf("pending/running must not be terminal")
	}
	for _, s := range []StepStatus{StepSucceeded, StepFailed, StepSkipped, StepTimedOut} {
		if !s.Terminal() {
			t.Errorf("%s must be terminal", s)
		}
	}
}
