package press

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/wyw14/solo-038/internal/model"
)

// DefaultStopTimeout bounds how long a stop command waits for zero-speed
// receipts from every press unit before the attempt is settled as timed out.
const DefaultStopTimeout = 5 * time.Second

// ErrStaleStopAcknowledgement is returned when a zero-speed receipt references
// a stop command that is no longer the active one. Late receipts from a
// previous command never count toward the current command.
var ErrStaleStopAcknowledgement = errors.New("stop acknowledgement belongs to another command")

const (
	StopUnitPending   = "pending"
	StopUnitConfirmed = "confirmed"
	StopUnitLate      = "late"
	StopUnitTimeout   = "timeout"
)

const (
	StopOutcomeInProgress = "in_progress"
	StopOutcomeComplete   = "complete"
	StopOutcomeTimedOut   = "timed_out"
)

// StopUnitStatus is the operator-visible receipt state of one press unit for
// the current stop command.
type StopUnitStatus struct {
	Unit   string    `json:"unit"`
	Status string    `json:"status"`
	At     time.Time `json:"at,omitempty"`
}

// StopStatus is a snapshot of the current stop command: which units have
// confirmed zero speed, which are still outstanding, and how the attempt
// settled. The zero value (Operation invalid) means no stop was issued yet.
type StopStatus struct {
	Operation   model.OperationID `json:"operation"`
	Epoch       uint64            `json:"epoch"`
	IssuedAt    time.Time         `json:"issued_at"`
	Deadline    time.Time         `json:"deadline"`
	Outcome     string            `json:"outcome"`
	Units       []StopUnitStatus  `json:"units"`
	Confirmed   []string          `json:"confirmed"`
	Pending     []string          `json:"pending"`
	CompletedAt time.Time         `json:"completed_at,omitempty"`
	SettledAt   time.Time         `json:"settled_at,omitempty"`
}

// Complete reports whether every press unit confirmed zero speed within the
// deadline of this stop command.
func (status StopStatus) Complete() bool {
	return status.Outcome == StopOutcomeComplete
}

type stopReceipt struct {
	status string
	at     time.Time
}

type registerSample struct {
	plateGeneration uint64
	offset          float64
}

type Coordinator struct {
	mu              sync.RWMutex
	controller      *Controller
	state           *State
	stopTimeout     time.Duration
	stopCommand     model.Command
	stopReceipts    map[string]stopReceipt
	stopCompletedAt time.Time
	stopSettledAt   time.Time
	registerSamples []registerSample
}

func NewCoordinator(controller *Controller, state *State) *Coordinator {
	return &Coordinator{controller: controller, state: state, stopTimeout: DefaultStopTimeout, stopReceipts: map[string]stopReceipt{}}
}

// SetStopTimeout overrides how long a stop command waits for per-unit
// zero-speed receipts. Non-positive values are ignored.
func (coordinator *Coordinator) SetStopTimeout(timeout time.Duration) {
	if timeout <= 0 {
		return
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	coordinator.stopTimeout = timeout
}

// BeginStop issues a new stop command. Receipts collected for any previous
// command are discarded, so a late receipt from an earlier command can never
// complete this one.
func (coordinator *Coordinator) BeginStop(job model.Job, epoch uint64, now time.Time) model.Command {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	coordinator.stopCommand = model.NewCommand(job, epoch, model.CommandStop, now)
	coordinator.stopReceipts = map[string]stopReceipt{}
	coordinator.stopCompletedAt = time.Time{}
	coordinator.stopSettledAt = time.Time{}
	return coordinator.stopCommand
}

// StopDeadline returns the instant by which every unit must have confirmed
// zero speed for the current stop command. It is zero when no stop was issued.
func (coordinator *Coordinator) StopDeadline() time.Time {
	coordinator.mu.RLock()
	defer coordinator.mu.RUnlock()
	return coordinator.stopDeadlineLocked()
}

func (coordinator *Coordinator) stopDeadlineLocked() time.Time {
	if !coordinator.stopCommand.Operation.Valid() {
		return time.Time{}
	}
	return coordinator.stopCommand.IssuedAt.Add(coordinator.stopTimeout)
}

// AcknowledgeStop records one unit's zero-speed receipt for the active stop
// command. Receipts for any other command are rejected with
// ErrStaleStopAcknowledgement; receipts arriving after the deadline of an
// incomplete command are marked late and rejected as well, so a timed-out
// stop stays locked.
func (coordinator *Coordinator) AcknowledgeStop(ack model.Acknowledgement) error {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if !coordinator.stopCommand.Operation.Valid() {
		return fmt.Errorf("no stop command is active")
	}
	if ack.Operation != coordinator.stopCommand.Operation || ack.Epoch != coordinator.stopCommand.Epoch {
		return ErrStaleStopAcknowledgement
	}
	if _, ok := coordinator.controller.Unit(ack.Unit); !ok {
		return fmt.Errorf("press unit is unknown")
	}
	if ack.Value != 0 {
		return fmt.Errorf("press unit has not reached zero speed")
	}
	if receipt, ok := coordinator.stopReceipts[ack.Unit]; ok && receipt.status == StopUnitConfirmed {
		return nil
	}
	if !coordinator.stopCompletedAt.IsZero() {
		return fmt.Errorf("stop command is already complete")
	}
	if deadline := coordinator.stopDeadlineLocked(); !deadline.IsZero() && ack.At.After(deadline) {
		coordinator.stopReceipts[ack.Unit] = stopReceipt{status: StopUnitLate, at: ack.At}
		return fmt.Errorf("stop acknowledgement for unit %s arrived after the stop deadline", ack.Unit)
	}
	coordinator.stopReceipts[ack.Unit] = stopReceipt{status: StopUnitConfirmed, at: ack.At}
	if coordinator.allConfirmedLocked() {
		coordinator.stopCompletedAt = ack.At
	}
	return nil
}

func (coordinator *Coordinator) allConfirmedLocked() bool {
	for _, unit := range coordinator.controller.Names() {
		receipt, ok := coordinator.stopReceipts[unit]
		if !ok || receipt.status != StopUnitConfirmed {
			return false
		}
	}
	return true
}

// evaluateLocked settles the current stop command against now: once the
// deadline passes with units still unconfirmed, those units are marked
// timed out and the command settles as timed_out. A settled command never
// reopens — only BeginStop starts a new attempt.
func (coordinator *Coordinator) evaluateLocked(now time.Time) {
	if !coordinator.stopCommand.Operation.Valid() || !coordinator.stopSettledAt.IsZero() {
		return
	}
	if !coordinator.stopCompletedAt.IsZero() {
		coordinator.stopSettledAt = coordinator.stopCompletedAt
		return
	}
	deadline := coordinator.stopDeadlineLocked()
	if deadline.IsZero() || now.Before(deadline) {
		return
	}
	for _, unit := range coordinator.controller.Names() {
		if _, ok := coordinator.stopReceipts[unit]; !ok {
			coordinator.stopReceipts[unit] = stopReceipt{status: StopUnitTimeout}
		}
	}
	coordinator.stopSettledAt = deadline
}

func (coordinator *Coordinator) statusLocked() StopStatus {
	status := StopStatus{
		Operation:   coordinator.stopCommand.Operation,
		Epoch:       coordinator.stopCommand.Epoch,
		IssuedAt:    coordinator.stopCommand.IssuedAt,
		Deadline:    coordinator.stopDeadlineLocked(),
		CompletedAt: coordinator.stopCompletedAt,
		SettledAt:   coordinator.stopSettledAt,
		Confirmed:   []string{},
		Pending:     []string{},
	}
	if !coordinator.stopCommand.Operation.Valid() {
		return status
	}
	switch {
	case !coordinator.stopCompletedAt.IsZero():
		status.Outcome = StopOutcomeComplete
	case !coordinator.stopSettledAt.IsZero():
		status.Outcome = StopOutcomeTimedOut
	default:
		status.Outcome = StopOutcomeInProgress
	}
	for _, unit := range coordinator.controller.Names() {
		receipt, ok := coordinator.stopReceipts[unit]
		if !ok {
			receipt = stopReceipt{status: StopUnitPending}
		}
		status.Units = append(status.Units, StopUnitStatus{Unit: unit, Status: receipt.status, At: receipt.at})
		if receipt.status == StopUnitConfirmed {
			status.Confirmed = append(status.Confirmed, unit)
		} else {
			status.Pending = append(status.Pending, unit)
		}
	}
	sort.Strings(status.Confirmed)
	sort.Strings(status.Pending)
	return status
}

// StopStatus evaluates the current stop command against now and returns its
// operator-visible snapshot.
func (coordinator *Coordinator) StopStatus(now time.Time) StopStatus {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	coordinator.evaluateLocked(now)
	return coordinator.statusLocked()
}

// CanSplice reports whether the splice interlock may open: every press unit
// must hold a confirmed zero-speed receipt for the current stop command. A
// missing or timed-out unit keeps the splice locked.
func (coordinator *Coordinator) CanSplice() bool {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	coordinator.evaluateLocked(time.Now())
	if !coordinator.stopCommand.Operation.Valid() || coordinator.stopCompletedAt.IsZero() {
		return false
	}
	return coordinator.allConfirmedLocked()
}

func (coordinator *Coordinator) ChangePlate(plate string) (uint64, error) {
	generation, err := coordinator.state.ChangePlate(plate)
	if err != nil {
		return 0, err
	}
	coordinator.mu.Lock()
	coordinator.registerSamples = nil
	coordinator.mu.Unlock()
	return generation, nil
}

func (coordinator *Coordinator) RecordRegister(plateGeneration uint64, offset float64) error {
	_, active, _, _ := coordinator.state.Snapshot()
	if plateGeneration != active {
		return fmt.Errorf("register sample belongs to another plate")
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	coordinator.registerSamples = append(coordinator.registerSamples, registerSample{plateGeneration: plateGeneration, offset: offset})
	if len(coordinator.registerSamples) > 20 {
		coordinator.registerSamples = coordinator.registerSamples[len(coordinator.registerSamples)-20:]
	}
	return nil
}

func (coordinator *Coordinator) RegisterStable(tolerance float64, minimum int) bool {
	coordinator.mu.RLock()
	defer coordinator.mu.RUnlock()
	_, active, _, _ := coordinator.state.Snapshot()
	count := 0
	for _, sample := range coordinator.registerSamples {
		if sample.plateGeneration == active && math.Abs(sample.offset) <= tolerance {
			count++
		}
	}
	return count >= minimum
}
