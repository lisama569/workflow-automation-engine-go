package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lisama569/workflow-automation-engine-go/pkg/workflow"
)

func TestCreateIfAbsentIsDedupGate(t *testing.T) {
	dir := t.TempDir()
	st, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	first := &workflow.RunState{TriggerID: "t1", Status: workflow.RunPending, CreatedAt: time.Now()}

	existing, err := st.CreateIfAbsent(ctx, first)
	if err != nil || existing != nil {
		t.Fatalf("first create: existing=%v err=%v", existing, err)
	}

	second := &workflow.RunState{TriggerID: "t1", Status: workflow.RunRunning}
	existing, err = st.CreateIfAbsent(ctx, second)
	if err != nil {
		t.Fatal(err)
	}
	if existing == nil || existing.Status != workflow.RunPending {
		t.Fatalf("concurrent duplicate must return first state, got %+v", existing)
	}

	loaded, err := st.Load(ctx, "t1")
	if err != nil || loaded.Status != workflow.RunPending {
		t.Fatalf("load after duplicate: %+v err=%v", loaded, err)
	}

	missing, err := st.Load(ctx, "does-not-exist")
	if err != nil || missing != nil {
		t.Fatalf("missing key: state=%v err=%v", missing, err)
	}
}

func TestSaveOverwrites(t *testing.T) {
	dir := t.TempDir()
	st, _ := NewFileStore(dir)
	ctx := context.Background()
	rs := &workflow.RunState{TriggerID: "t2", Status: workflow.RunPending, Epoch: 1}
	if _, err := st.CreateIfAbsent(ctx, rs); err != nil {
		t.Fatal(err)
	}
	rs.Status = workflow.RunSucceeded
	rs.Epoch = 5
	if err := st.Save(ctx, rs); err != nil {
		t.Fatal(err)
	}
	loaded, err := st.Load(ctx, "t2")
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Status != workflow.RunSucceeded || loaded.Epoch != 5 {
		t.Fatalf("overwritten state not persisted: %+v", loaded)
	}
}

var _ = errors.New
