package service

import (
	"context"
	"testing"
	"time"

	"github.com/wyw14/solo-038/internal/model"
	"github.com/wyw14/solo-038/internal/press"
)

func newTestRuntime(t *testing.T, stopTimeout time.Duration) *Runtime {
	t.Helper()
	runtime, err := NewRuntime(Config{
		DataDir:           t.TempDir(),
		FestoonCapacity:   120,
		RegisterGap:       50 * time.Millisecond,
		RegisterTolerance: 0.25,
		StopAckTimeout:    stopTimeout,
	})
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	if err := runtime.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	return runtime
}

func printingJob(t *testing.T, runtime *Runtime) model.Job {
	t.Helper()
	current, ok := runtime.jobs.Current()
	if !ok {
		t.Fatal("no current job")
	}
	now := time.Now().UTC()
	var err error
	for _, next := range []model.JobState{model.JobThreading, model.JobRegistering, model.JobPrinting} {
		current, err = current.Transition(next, now)
		if err != nil {
			t.Fatalf("transition to %s: %v", next, err)
		}
	}
	if err := runtime.jobs.Put(current); err != nil {
		t.Fatalf("put printing job: %v", err)
	}
	return current
}

func TestStopPressOperationWaitsForEveryUnit(t *testing.T) {
	runtime := newTestRuntime(t, time.Second)
	current := printingJob(t, runtime)
	if err := runtime.stopPressOperation(context.Background(), current); err != nil {
		t.Fatalf("stopPressOperation: %v", err)
	}
	status := runtime.PressStopStatus()
	if status.Phase != press.StopComplete || status.Result != "complete" || !status.CanSplice {
		t.Fatalf("status = %+v, want complete splice-ready", status)
	}
	if len(status.Confirmed) != 4 || len(status.Outstanding) != 0 {
		t.Fatalf("confirmed=%v outstanding=%v, want all four confirmed", status.Confirmed, status.Outstanding)
	}
}

func TestStopPressOperationLocksSpliceWhenReceiptsMissDeadline(t *testing.T) {
	// Timeout shorter than the earliest independent unit receipt, so every
	// receipt is late and the splice must remain locked.
	runtime := newTestRuntime(t, time.Millisecond)
	current := printingJob(t, runtime)
	err := runtime.stopPressOperation(context.Background(), current)
	if err == nil {
		t.Fatal("stop must fail when units miss the deadline")
	}
	status := runtime.PressStopStatus()
	if status.Phase != press.StopTimedOut || status.CanSplice {
		t.Fatalf("status = %+v, want timed_out with splice locked", status)
	}
	if len(status.Outstanding) != 4 {
		t.Fatalf("outstanding = %v, want all four units unconfirmed", status.Outstanding)
	}
	if runtime.pressControl.CanSplice() {
		t.Fatal("splice interlock must stay closed after an incomplete stop")
	}
}

func TestStaleReceiptAfterRetriedStopDoesNotBackfill(t *testing.T) {
	runtime := newTestRuntime(t, time.Second)
	current := printingJob(t, runtime)

	command1, err := runtime.pressControl.BeginStop(current, 101, time.Second, time.Now().UTC())
	if err != nil {
		t.Fatalf("BeginStop: %v", err)
	}
	// Operator retries: a second command supersedes the first.
	command2, err := runtime.pressControl.BeginStop(current, 102, time.Second, time.Now().UTC())
	if err != nil {
		t.Fatalf("BeginStop retry: %v", err)
	}
	// A late receipt carrying the first command identity arrives during the
	// second command's window.
	ack := model.Acknowledgement{Unit: "cyan", Operation: command1.Operation, Epoch: command1.Epoch, Value: 0, At: time.Now().UTC()}
	if err := runtime.pressControl.AcknowledgeStop(ack); err == nil {
		t.Fatal("receipt from the previous stop command must be rejected")
	}
	status := runtime.PressStopStatus()
	if status.Operation != command2.Operation || len(status.Confirmed) != 0 {
		t.Fatalf("status = %+v, new command must not be credited by old receipt", status)
	}
	if got := status.Stale[len(status.Stale)-1]; got.Reason != press.StalePreviousCommand {
		t.Fatalf("stale reason = %q, want %q", got.Reason, press.StalePreviousCommand)
	}
}
