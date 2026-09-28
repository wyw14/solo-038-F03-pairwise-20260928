package press

import (
	"errors"
	"testing"
	"time"

	"github.com/wyw14/solo-038/internal/model"
)

func newTestCoordinator(t *testing.T) *Coordinator {
	t.Helper()
	controller, err := NewController("cyan", "magenta", "yellow", "black")
	if err != nil {
		t.Fatal(err)
	}
	coordinator := NewCoordinator(controller, NewState())
	coordinator.SetStopTimeout(100 * time.Millisecond)
	return coordinator
}

func zeroAck(command model.Command, unit string, at time.Time) model.Acknowledgement {
	return model.Acknowledgement{Unit: unit, Operation: command.Operation, Epoch: command.Epoch, Value: 0, At: at}
}

func TestStopReceiptsCompleteWhenEveryUnitConfirmed(t *testing.T) {
	coordinator := newTestCoordinator(t)
	issued := time.Date(2026, 9, 27, 10, 0, 0, 0, time.UTC)
	command := coordinator.BeginStop(model.NewJob(issued), 1, issued)

	for index, unit := range coordinator.controller.Names() {
		if err := coordinator.AcknowledgeStop(zeroAck(command, unit, issued.Add(time.Duration(index+1)*10*time.Millisecond))); err != nil {
			t.Fatalf("acknowledge %s: %v", unit, err)
		}
	}

	status := coordinator.StopStatus(issued.Add(50 * time.Millisecond))
	if status.Outcome != StopOutcomeComplete {
		t.Fatalf("outcome = %q, want %q", status.Outcome, StopOutcomeComplete)
	}
	if len(status.Pending) != 0 {
		t.Fatalf("pending units = %v, want none", status.Pending)
	}
	if len(status.Confirmed) != 4 {
		t.Fatalf("confirmed units = %v, want all four", status.Confirmed)
	}
	if !coordinator.CanSplice() {
		t.Fatal("CanSplice = false, want true after all four zero-speed receipts")
	}
	if status.CompletedAt.IsZero() {
		t.Fatal("completed time was not recorded")
	}
}

func TestStopReceiptsPartialShowsPendingUnits(t *testing.T) {
	coordinator := newTestCoordinator(t)
	issued := time.Date(2026, 9, 27, 10, 0, 0, 0, time.UTC)
	command := coordinator.BeginStop(model.NewJob(issued), 1, issued)

	for _, unit := range []string{"cyan", "magenta", "black"} {
		if err := coordinator.AcknowledgeStop(zeroAck(command, unit, issued.Add(10*time.Millisecond))); err != nil {
			t.Fatalf("acknowledge %s: %v", unit, err)
		}
	}

	status := coordinator.StopStatus(issued.Add(50 * time.Millisecond))
	if status.Outcome != StopOutcomeInProgress {
		t.Fatalf("outcome = %q, want %q", status.Outcome, StopOutcomeInProgress)
	}
	if len(status.Pending) != 1 || status.Pending[0] != "yellow" {
		t.Fatalf("pending = %v, want only [yellow]", status.Pending)
	}
	if coordinator.CanSplice() {
		t.Fatal("CanSplice = true while a unit has not confirmed zero speed")
	}
}

func TestStopReceiptsSettleTimedOutAndLateReceiptStaysLocked(t *testing.T) {
	coordinator := newTestCoordinator(t)
	issued := time.Date(2026, 9, 27, 10, 0, 0, 0, time.UTC)
	command := coordinator.BeginStop(model.NewJob(issued), 1, issued)

	for _, unit := range []string{"cyan", "magenta", "black"} {
		if err := coordinator.AcknowledgeStop(zeroAck(command, unit, issued.Add(10*time.Millisecond))); err != nil {
			t.Fatalf("acknowledge %s: %v", unit, err)
		}
	}

	status := coordinator.StopStatus(issued.Add(150 * time.Millisecond))
	if status.Outcome != StopOutcomeTimedOut {
		t.Fatalf("outcome = %q, want %q", status.Outcome, StopOutcomeTimedOut)
	}
	if coordinator.CanSplice() {
		t.Fatal("CanSplice = true after a timed-out stop")
	}
	unitStatus := map[string]string{}
	for _, unit := range status.Units {
		unitStatus[unit.Unit] = unit.Status
	}
	if unitStatus["yellow"] != StopUnitTimeout {
		t.Fatalf("yellow status = %q, want %q", unitStatus["yellow"], StopUnitTimeout)
	}

	// The missing unit's receipt finally arrives — too late. It must not
	// complete the stop or open the splice.
	if err := coordinator.AcknowledgeStop(zeroAck(command, "yellow", issued.Add(200*time.Millisecond))); err == nil {
		t.Fatal("late acknowledgement after the deadline was accepted")
	}
	status = coordinator.StopStatus(issued.Add(250 * time.Millisecond))
	if status.Outcome != StopOutcomeTimedOut {
		t.Fatalf("outcome after late receipt = %q, want still %q", status.Outcome, StopOutcomeTimedOut)
	}
	if coordinator.CanSplice() {
		t.Fatal("late receipt opened the splice interlock")
	}
}

func TestStaleReceiptFromPreviousCommandCannotCountForNewCommand(t *testing.T) {
	coordinator := newTestCoordinator(t)
	issued := time.Date(2026, 9, 27, 10, 0, 0, 0, time.UTC)
	first := coordinator.BeginStop(model.NewJob(issued), 1, issued)
	for _, unit := range []string{"cyan", "magenta", "black"} {
		if err := coordinator.AcknowledgeStop(zeroAck(first, unit, issued.Add(10*time.Millisecond))); err != nil {
			t.Fatalf("acknowledge %s on first command: %v", unit, err)
		}
	}
	coordinator.StopStatus(issued.Add(150 * time.Millisecond)) // settle first command as timed out

	// New stop command: the three earlier receipts must be discarded.
	reissued := issued.Add(5 * time.Minute)
	second := coordinator.BeginStop(model.NewJob(reissued), 2, reissued)
	status := coordinator.StopStatus(reissued)
	if len(status.Pending) != 4 {
		t.Fatalf("pending after new command = %v, want all four units pending", status.Pending)
	}
	if coordinator.CanSplice() {
		t.Fatal("CanSplice = true for a fresh command with no receipts")
	}

	// A late receipt carrying the FIRST command identity arrives during the
	// second command. It is rejected and must not fill cyan's slot.
	err := coordinator.AcknowledgeStop(zeroAck(first, "cyan", reissued.Add(5*time.Millisecond)))
	if !errors.Is(err, ErrStaleStopAcknowledgement) {
		t.Fatalf("stale receipt error = %v, want ErrStaleStopAcknowledgement", err)
	}
	status = coordinator.StopStatus(reissued.Add(10 * time.Millisecond))
	if len(status.Pending) != 4 {
		t.Fatalf("pending after stale receipt = %v, stale receipt counted toward the new command", status.Pending)
	}

	// Even receipts for every unit under the OLD identity cannot complete it.
	for _, unit := range coordinator.controller.Names() {
		if err := coordinator.AcknowledgeStop(zeroAck(first, unit, reissued.Add(20*time.Millisecond))); !errors.Is(err, ErrStaleStopAcknowledgement) {
			t.Fatalf("stale receipt for %s: error = %v", unit, err)
		}
	}
	if coordinator.CanSplice() {
		t.Fatal("old-command receipts opened the splice for the new command")
	}

	// The new command completes only with receipts carrying its own identity.
	for index, unit := range coordinator.controller.Names() {
		if err := coordinator.AcknowledgeStop(zeroAck(second, unit, reissued.Add(time.Duration(index+1)*10*time.Millisecond))); err != nil {
			t.Fatalf("acknowledge %s on new command: %v", unit, err)
		}
	}
	if !coordinator.CanSplice() {
		t.Fatal("CanSplice = false after every unit confirmed the new command")
	}
}

func TestNonZeroAndUnknownReceiptsAreRejected(t *testing.T) {
	coordinator := newTestCoordinator(t)
	issued := time.Date(2026, 9, 27, 10, 0, 0, 0, time.UTC)
	command := coordinator.BeginStop(model.NewJob(issued), 1, issued)

	moving := zeroAck(command, "cyan", issued.Add(10*time.Millisecond))
	moving.Value = 3.5
	if err := coordinator.AcknowledgeStop(moving); err == nil {
		t.Fatal("non-zero-speed receipt was accepted")
	}
	if err := coordinator.AcknowledgeStop(zeroAck(command, "varnish", issued.Add(10*time.Millisecond))); err == nil {
		t.Fatal("unknown unit receipt was accepted")
	}
	status := coordinator.StopStatus(issued.Add(20 * time.Millisecond))
	if len(status.Confirmed) != 0 {
		t.Fatalf("confirmed = %v, rejected receipts must not be recorded", status.Confirmed)
	}
}

func TestDuplicateReceiptIsIdempotent(t *testing.T) {
	coordinator := newTestCoordinator(t)
	issued := time.Date(2026, 9, 27, 10, 0, 0, 0, time.UTC)
	command := coordinator.BeginStop(model.NewJob(issued), 1, issued)

	ack := zeroAck(command, "cyan", issued.Add(10*time.Millisecond))
	if err := coordinator.AcknowledgeStop(ack); err != nil {
		t.Fatalf("first receipt: %v", err)
	}
	if err := coordinator.AcknowledgeStop(ack); err != nil {
		t.Fatalf("duplicate receipt: %v", err)
	}
	status := coordinator.StopStatus(issued.Add(20 * time.Millisecond))
	if len(status.Confirmed) != 1 || status.Confirmed[0] != "cyan" {
		t.Fatalf("confirmed = %v, want only cyan once", status.Confirmed)
	}
}

func TestCanSpliceWithoutStopCommand(t *testing.T) {
	coordinator := newTestCoordinator(t)
	if coordinator.CanSplice() {
		t.Fatal("CanSplice = true before any stop command was issued")
	}
	status := coordinator.StopStatus(time.Now())
	if status.Operation.Valid() {
		t.Fatal("stop status reports an operation before any stop command was issued")
	}
}
