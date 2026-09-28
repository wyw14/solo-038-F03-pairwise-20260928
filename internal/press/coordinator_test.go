package press

import (
	"context"
	"testing"
	"time"

	"github.com/wyw14/solo-038/internal/model"
)

var unitNames = []string{"black", "cyan", "magenta", "yellow"}

func newTestCoordinator(t *testing.T) *Coordinator {
	t.Helper()
	controller, err := NewController(unitNames...)
	if err != nil {
		t.Fatalf("NewController: %v", err)
	}
	return NewCoordinator(controller, NewState())
}

func testJob() model.Job {
	return model.NewJob(time.Unix(1_700_000_000, 0))
}

func ackFor(command model.Command, unit string, at time.Time, value float64) model.Acknowledgement {
	return model.Acknowledgement{Unit: unit, Operation: command.Operation, Epoch: command.Epoch, Value: value, At: at}
}

func TestSequentialReceiptsUnlockSplice(t *testing.T) {
	coordinator := newTestCoordinator(t)
	now := time.Now().UTC()
	command, err := coordinator.BeginStop(testJob(), 1, time.Second, now)
	if err != nil {
		t.Fatalf("BeginStop: %v", err)
	}
	if coordinator.CanSplice() {
		t.Fatal("splice must stay locked before any receipt")
	}
	confirmed := []string{}
	for index, unit := range unitNames {
		if err := coordinator.AcknowledgeStop(ackFor(command, unit, now.Add(time.Millisecond), 0)); err != nil {
			t.Fatalf("ack %s: %v", unit, err)
		}
		confirmed = append(confirmed, unit)
		status := coordinator.StopStatus(now.Add(time.Millisecond))
		if status.Phase != StopAwaiting && status.Phase != StopComplete {
			t.Fatalf("unexpected phase %q", status.Phase)
		}
		// Until every unit is in, the splice stays locked.
		if index < len(unitNames)-1 {
			if coordinator.CanSplice() {
				t.Fatalf("splice unlocked after only %d of %d units", index+1, len(unitNames))
			}
			if len(status.Outstanding) != len(unitNames)-len(confirmed) {
				t.Fatalf("outstanding = %v, want %d missing", status.Outstanding, len(unitNames)-len(confirmed))
			}
		}
	}
	status := coordinator.StopStatus(now.Add(time.Millisecond))
	if status.Phase != StopComplete || status.Result != "complete" || !status.CanSplice {
		t.Fatalf("status = %+v, want complete splice-ready", status)
	}
	if !coordinator.CanSplice() {
		t.Fatal("splice must unlock once all four units confirm")
	}
	if len(status.Outstanding) != 0 || len(status.Confirmed) != 4 {
		t.Fatalf("confirmed=%v outstanding=%v", status.Confirmed, status.Outstanding)
	}
}

func TestTimeoutKeepsSpliceLockedAndReportsOutstanding(t *testing.T) {
	coordinator := newTestCoordinator(t)
	now := time.Now().UTC()
	command, err := coordinator.BeginStop(testJob(), 7, 60*time.Millisecond, now)
	if err != nil {
		t.Fatalf("BeginStop: %v", err)
	}
	// Three of four units confirm in time; black never does.
	for _, unit := range []string{"cyan", "magenta", "yellow"} {
		if err := coordinator.AcknowledgeStop(ackFor(command, unit, now.Add(time.Millisecond), 0)); err != nil {
			t.Fatalf("ack %s: %v", unit, err)
		}
	}
	status := coordinator.AwaitStop(context.Background(), now)
	if status.Phase != StopTimedOut || status.Result != "incomplete_timeout" {
		t.Fatalf("phase = %q result = %q, want timed_out", status.Phase, status.Result)
	}
	if coordinator.CanSplice() || status.CanSplice {
		t.Fatal("splice must stay locked when a unit is missing after timeout")
	}
	if len(status.Outstanding) != 1 || status.Outstanding[0] != "black" {
		t.Fatalf("outstanding = %v, want [black]", status.Outstanding)
	}
	// The late receipt for the expired command is rejected, not backfilled.
	late := ackFor(command, "black", now.Add(time.Second), 0)
	if err := coordinator.AcknowledgeStop(late); err == nil {
		t.Fatal("late acknowledgement after deadline must be rejected")
	}
	status = coordinator.StopStatus(now.Add(2 * time.Second))
	if coordinator.CanSplice() {
		t.Fatal("late receipt must not unlock the splice")
	}
	if len(status.Stale) != 1 || status.Stale[0].Reason != StaleAfterDeadline || status.Stale[0].Unit != "black" {
		t.Fatalf("stale = %+v, want one after_deadline receipt for black", status.Stale)
	}
}

func TestPreviousCommandReceiptCannotCreditNewCommand(t *testing.T) {
	coordinator := newTestCoordinator(t)
	now := time.Now().UTC()
	first, err := coordinator.BeginStop(testJob(), 1, time.Second, now)
	if err != nil {
		t.Fatalf("BeginStop first: %v", err)
	}
	if err := coordinator.AcknowledgeStop(ackFor(first, "cyan", now.Add(time.Millisecond), 0)); err != nil {
		t.Fatalf("ack cyan on first command: %v", err)
	}
	// A new stop command supersedes the first before the other units answer.
	second, err := coordinator.BeginStop(testJob(), 2, time.Second, now.Add(10*time.Millisecond))
	if err != nil {
		t.Fatalf("BeginStop second: %v", err)
	}
	for _, unit := range []string{"magenta", "yellow", "black"} {
		// Late receipts still carrying the FIRST command identity.
		if err := coordinator.AcknowledgeStop(ackFor(first, unit, now.Add(20*time.Millisecond), 0)); err == nil {
			t.Fatalf("stale receipt for %s must be rejected", unit)
		}
	}
	status := coordinator.StopStatus(now.Add(20 * time.Millisecond))
	if status.Operation != second.Operation || status.Epoch != second.Epoch {
		t.Fatal("status must track the newest command")
	}
	if coordinator.CanSplice() {
		t.Fatal("stale receipts from the previous command must not fill the new command")
	}
	if len(status.Confirmed) != 0 || len(status.Outstanding) != 4 {
		t.Fatalf("confirmed=%v outstanding=%v, new command must start empty", status.Confirmed, status.Outstanding)
	}
	if len(status.Stale) != 3 {
		t.Fatalf("stale = %+v, want three previous_command receipts", status.Stale)
	}
	for _, receipt := range status.Stale {
		if receipt.Reason != StalePreviousCommand || receipt.Operation != first.Operation {
			t.Fatalf("stale receipt = %+v", receipt)
		}
	}
}

func TestInvalidReceiptsAreRejected(t *testing.T) {
	coordinator := newTestCoordinator(t)
	now := time.Now().UTC()
	command, err := coordinator.BeginStop(testJob(), 1, time.Second, now)
	if err != nil {
		t.Fatalf("BeginStop: %v", err)
	}
	unknown := ackFor(command, "varnish", now, 0)
	if err := coordinator.AcknowledgeStop(unknown); err == nil {
		t.Fatal("unknown unit must be rejected")
	}
	moving := ackFor(command, "cyan", now, 4.5)
	if err := coordinator.AcknowledgeStop(moving); err == nil {
		t.Fatal("non-zero speed receipt must be rejected")
	}
	status := coordinator.StopStatus(now)
	if len(status.Confirmed) != 0 {
		t.Fatalf("confirmed = %v, invalid receipts must not be recorded", status.Confirmed)
	}
}

func TestAwaitStopReturnsWhenAllUnitsConfirm(t *testing.T) {
	coordinator := newTestCoordinator(t)
	now := time.Now().UTC()
	command, err := coordinator.BeginStop(testJob(), 1, time.Second, now)
	if err != nil {
		t.Fatalf("BeginStop: %v", err)
	}
	go func() {
		for index, unit := range unitNames {
			time.Sleep(time.Duration(index+1) * 2 * time.Millisecond)
			_ = coordinator.AcknowledgeStop(ackFor(command, unit, time.Now().UTC(), 0))
		}
	}()
	status := coordinator.AwaitStop(context.Background(), now)
	if status.Phase != StopComplete || !coordinator.CanSplice() {
		t.Fatalf("status = %+v, want complete after concurrent receipts", status)
	}
}

func TestAwaitStopHonoursContextCancellation(t *testing.T) {
	coordinator := newTestCoordinator(t)
	now := time.Now().UTC()
	command, err := coordinator.BeginStop(testJob(), 1, 5*time.Second, now)
	if err != nil {
		t.Fatalf("BeginStop: %v", err)
	}
	if err := coordinator.AcknowledgeStop(ackFor(command, "cyan", now, 0)); err != nil {
		t.Fatalf("ack: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	status := coordinator.AwaitStop(ctx, now)
	if status.Phase != StopAwaiting {
		t.Fatalf("phase = %q, want still awaiting after cancellation", status.Phase)
	}
	if coordinator.CanSplice() {
		t.Fatal("cancelled wait with missing units must not unlock splice")
	}
}

func TestIdleStatusHasNoCommand(t *testing.T) {
	coordinator := newTestCoordinator(t)
	status := coordinator.StopStatus(time.Now().UTC())
	if status.Phase != StopIdle || status.Result != "no_stop_command" || status.Active || status.CanSplice {
		t.Fatalf("idle status = %+v", status)
	}
	if coordinator.CanSplice() {
		t.Fatal("splice cannot be ready before a stop command")
	}
}
