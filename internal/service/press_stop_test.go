package service

import (
	"context"
	"testing"
	"time"

	"github.com/wyw14/solo-038/internal/model"
)

func newStopTestRuntime(t *testing.T) *Runtime {
	t.Helper()
	runtime, err := NewRuntime(Config{DataDir: t.TempDir(), FestoonCapacity: 120, RegisterGap: 500 * time.Millisecond, RegisterTolerance: 0.25})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	return runtime
}

func moveJobToPrinting(t *testing.T, runtime *Runtime) {
	t.Helper()
	current, ok := runtime.CurrentJob()
	if !ok {
		t.Fatal("no current job")
	}
	for _, next := range []model.JobState{model.JobThreading, model.JobRegistering, model.JobPrinting} {
		updated, err := current.Transition(next, time.Now())
		if err != nil {
			t.Fatalf("transition to %s: %v", next, err)
		}
		if err := runtime.jobs.Put(updated); err != nil {
			t.Fatal(err)
		}
		current = updated
	}
}

func TestPressStopBeforeAnyStopCommand(t *testing.T) {
	runtime := newStopTestRuntime(t)
	if _, ok := runtime.PressStop(); ok {
		t.Fatal("PressStop ok = true before any stop command was issued")
	}
}

func TestStopPressOperationCollectsPerUnitReceipts(t *testing.T) {
	runtime := newStopTestRuntime(t)
	moveJobToPrinting(t, runtime)

	if err := runtime.ExecuteOperation(context.Background(), "stop_press"); err != nil {
		t.Fatalf("stop_press: %v", err)
	}

	status, ok := runtime.PressStop()
	if !ok {
		t.Fatal("PressStop ok = false after a stop command")
	}
	if status.Outcome != "complete" {
		t.Fatalf("outcome = %q, want complete", status.Outcome)
	}
	if len(status.Confirmed) != 4 || len(status.Pending) != 0 {
		t.Fatalf("confirmed = %v pending = %v, want all four confirmed", status.Confirmed, status.Pending)
	}
	if !status.CanSplice {
		t.Fatal("can_splice = false after every unit confirmed zero speed")
	}

	spliceLock := false
	for _, interlock := range runtime.Interlocks() {
		if interlock.Name == "press_stopped_for_splice" {
			spliceLock = interlock.Satisfied
		}
	}
	if !spliceLock {
		t.Fatal("press_stopped_for_splice interlock is not satisfied after a complete stop")
	}
}

func TestSecondStopCommandRelocksSplice(t *testing.T) {
	runtime := newStopTestRuntime(t)
	moveJobToPrinting(t, runtime)

	if err := runtime.ExecuteOperation(context.Background(), "stop_press"); err != nil {
		t.Fatalf("first stop_press: %v", err)
	}
	first, _ := runtime.PressStop()
	if !first.CanSplice {
		t.Fatal("can_splice = false after first complete stop")
	}

	// A new stop command starts a fresh receipt collection: the receipts of
	// the previous command must not carry over.
	runtime.pressControl.BeginStop(mustCurrentJob(t, runtime), uint64(time.Now().UnixNano()), time.Now())
	status := runtime.pressControl.StopStatus(time.Now())
	if len(status.Pending) != 4 {
		t.Fatalf("pending after reissued stop = %v, want all four units pending again", status.Pending)
	}
	if runtime.pressControl.CanSplice() {
		t.Fatal("CanSplice = true for a fresh stop command with no receipts")
	}

	// A late receipt from the previous command arrives now — it must be
	// rejected and must not count toward the new command.
	stale := model.Acknowledgement{Unit: "cyan", Operation: model.OperationID(first.Operation), Epoch: first.Epoch, Value: 0, At: time.Now()}
	if err := runtime.pressControl.AcknowledgeStop(stale); err == nil {
		t.Fatal("stale receipt from the previous command was accepted")
	}
	if runtime.pressControl.CanSplice() {
		t.Fatal("stale receipt opened the splice for the new command")
	}
}

func mustCurrentJob(t *testing.T, runtime *Runtime) model.Job {
	t.Helper()
	current, ok := runtime.CurrentJob()
	if !ok {
		t.Fatal("no current job")
	}
	return current
}
