package press

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/wyw14/solo-038/internal/model"
)

type registerSample struct {
	plateGeneration uint64
	offset          float64
}

// StopPhase is the state of the per-unit stop receipt collection.
type StopPhase string

const (
	StopIdle     StopPhase = "idle"
	StopAwaiting StopPhase = "awaiting"
	StopComplete StopPhase = "complete"
	StopTimedOut StopPhase = "timed_out"
)

const maxStaleReceipts = 16

// Stale receipt rejection reasons.
const (
	StalePreviousCommand = "previous_command"
	StaleAfterDeadline   = "after_deadline"
)

// StaleReceipt records a zero-speed acknowledgement that arrived but could not
// be counted for the current stop command: it belonged to an earlier command
// or reached the coordinator after the deadline.
type StaleReceipt struct {
	Unit      string            `json:"unit"`
	Operation model.OperationID `json:"operation"`
	Epoch     uint64            `json:"epoch"`
	At        time.Time         `json:"at"`
	Reason    string            `json:"reason"`
}

// StopStatus is the operator-facing view of the current stop command.
type StopStatus struct {
	Active      bool              `json:"active"`
	Operation   model.OperationID `json:"operation"`
	Epoch       uint64            `json:"epoch"`
	Phase       StopPhase         `json:"phase"`
	Result      string            `json:"result"`
	Required    []string          `json:"required_units"`
	Confirmed   []string          `json:"confirmed_units"`
	Outstanding []string          `json:"outstanding_units"`
	Stale       []StaleReceipt    `json:"stale_receipts"`
	Deadline    time.Time         `json:"deadline"`
	CanSplice   bool              `json:"can_splice"`
}

type Coordinator struct {
	mu              sync.RWMutex
	notify          chan struct{}
	controller      *Controller
	state           *State
	stopCommand     model.Command
	stopDeadline    time.Time
	stopPhase       StopPhase
	stopProof       map[string]model.Acknowledgement
	staleReceipts   []StaleReceipt
	registerSamples []registerSample
}

func NewCoordinator(controller *Controller, state *State) *Coordinator {
	coordinator := &Coordinator{
		controller: controller,
		state:      state,
		notify:     make(chan struct{}),
		stopPhase:  StopIdle,
		stopProof:  map[string]model.Acknowledgement{},
	}
	return coordinator
}

// signalLocked wakes everyone waiting on receipt/deadline progress. The
// notifier is closed (not sent to) so any number of waiters can observe it.
func (coordinator *Coordinator) signalLocked() {
	close(coordinator.notify)
	coordinator.notify = make(chan struct{})
}

// BeginStop issues a new stop command and opens a fresh per-unit receipt
// window closing at now+timeout. Receipts from any earlier command can never
// fill this window because each command carries its own operation identity.
func (coordinator *Coordinator) BeginStop(job model.Job, epoch uint64, timeout time.Duration, now time.Time) (model.Command, error) {
	if timeout <= 0 {
		return model.Command{}, fmt.Errorf("stop acknowledgement timeout must be positive")
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	coordinator.stopCommand = model.NewCommand(job, epoch, model.CommandStop, now)
	coordinator.stopDeadline = now.Add(timeout)
	coordinator.stopPhase = StopAwaiting
	coordinator.stopProof = map[string]model.Acknowledgement{}
	coordinator.signalLocked()
	return coordinator.stopCommand, nil
}

// AcknowledgeStop files one printing unit's zero-speed receipt for the current
// stop command. Receipts for a previous command or receipts arriving after the
// deadline are recorded as stale and never counted toward the new command.
func (coordinator *Coordinator) AcknowledgeStop(ack model.Acknowledgement) error {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if !coordinator.stopCommand.Operation.Valid() {
		return fmt.Errorf("stop acknowledgement received without a stop command")
	}
	if _, ok := coordinator.controller.Unit(ack.Unit); !ok {
		return fmt.Errorf("press unit is unknown")
	}
	if ack.Operation != coordinator.stopCommand.Operation || ack.Epoch != coordinator.stopCommand.Epoch {
		coordinator.recordStaleLocked(StaleReceipt{Unit: ack.Unit, Operation: ack.Operation, Epoch: ack.Epoch, At: ack.At.UTC(), Reason: StalePreviousCommand})
		return fmt.Errorf("stop acknowledgement belongs to another command")
	}
	if ack.Value != 0 {
		return fmt.Errorf("press unit has not reached zero speed")
	}
	coordinator.expireLocked(time.Now().UTC())
	if coordinator.stopPhase == StopTimedOut || (!ack.At.UTC().IsZero() && ack.At.UTC().After(coordinator.stopDeadline)) {
		coordinator.recordStaleLocked(StaleReceipt{Unit: ack.Unit, Operation: ack.Operation, Epoch: ack.Epoch, At: ack.At.UTC(), Reason: StaleAfterDeadline})
		coordinator.signalLocked()
		return fmt.Errorf("stop acknowledgement arrived after the deadline")
	}
	if _, exists := coordinator.stopProof[ack.Unit]; !exists {
		coordinator.stopProof[ack.Unit] = ack
	}
	if coordinator.stopPhase == StopAwaiting && coordinator.allConfirmedLocked() {
		coordinator.stopPhase = StopComplete
	}
	coordinator.signalLocked()
	return nil
}

// AwaitStop blocks until every required unit has confirmed zero speed, the
// deadline passes, or ctx is cancelled. The returned status always reflects
// the final stop result and is safe to show the operator.
func (coordinator *Coordinator) AwaitStop(ctx context.Context, now time.Time) StopStatus {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	coordinator.expireLocked(now)
	for coordinator.stopPhase == StopAwaiting && ctx.Err() == nil {
		wait := coordinator.stopDeadline.Sub(now)
		if wait <= 0 {
			coordinator.expireLocked(time.Now().UTC())
			break
		}
		changes := coordinator.notify
		timer := time.NewTimer(wait)
		coordinator.mu.Unlock()
		select {
		case <-changes:
		case <-timer.C:
		case <-ctx.Done():
		}
		timer.Stop()
		coordinator.mu.Lock()
		now = time.Now().UTC()
		coordinator.expireLocked(now)
	}
	return coordinator.statusLocked(time.Now().UTC())
}

// CanSplice reports whether the splice interlock may open: only a fully
// confirmed stop command for the current epoch unlocks it. A timeout or a
// single missing unit keeps the splice locked.
func (coordinator *Coordinator) CanSplice() bool {
	coordinator.mu.RLock()
	defer coordinator.mu.RUnlock()
	return coordinator.spliceAllowedLocked()
}

// StopStatus returns a snapshot of the active stop command, including the
// units that have not yet confirmed zero speed and the outcome of the stop.
func (coordinator *Coordinator) StopStatus(now time.Time) StopStatus {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	coordinator.expireLocked(now)
	return coordinator.statusLocked(now)
}

func (coordinator *Coordinator) expireLocked(now time.Time) {
	if coordinator.stopPhase == StopAwaiting && !now.Before(coordinator.stopDeadline) {
		coordinator.stopPhase = StopTimedOut
		coordinator.signalLocked()
	}
}

func (coordinator *Coordinator) spliceAllowedLocked() bool {
	if coordinator.stopPhase != StopComplete || !coordinator.stopCommand.Operation.Valid() {
		return false
	}
	for _, unit := range coordinator.controller.Names() {
		proof, ok := coordinator.stopProof[unit]
		if !ok || proof.Operation != coordinator.stopCommand.Operation || proof.Epoch != coordinator.stopCommand.Epoch || proof.Value != 0 {
			return false
		}
	}
	return true
}

func (coordinator *Coordinator) allConfirmedLocked() bool {
	for _, unit := range coordinator.controller.Names() {
		if _, ok := coordinator.stopProof[unit]; !ok {
			return false
		}
	}
	return true
}

func (coordinator *Coordinator) recordStaleLocked(receipt StaleReceipt) {
	coordinator.staleReceipts = append(coordinator.staleReceipts, receipt)
	if len(coordinator.staleReceipts) > maxStaleReceipts {
		coordinator.staleReceipts = coordinator.staleReceipts[len(coordinator.staleReceipts)-maxStaleReceipts:]
	}
}

func (coordinator *Coordinator) statusLocked(now time.Time) StopStatus {
	required := coordinator.controller.Names()
	confirmed := []string{}
	outstanding := []string{}
	if coordinator.stopPhase != StopIdle {
		for _, unit := range required {
			if _, ok := coordinator.stopProof[unit]; ok {
				confirmed = append(confirmed, unit)
			} else {
				outstanding = append(outstanding, unit)
			}
		}
	}
	sort.Strings(confirmed)
	sort.Strings(outstanding)
	result := "no_stop_command"
	switch coordinator.stopPhase {
	case StopAwaiting:
		result = "pending"
	case StopComplete:
		result = "complete"
	case StopTimedOut:
		result = "incomplete_timeout"
	}
	status := StopStatus{
		Active:      coordinator.stopPhase == StopAwaiting,
		Operation:   coordinator.stopCommand.Operation,
		Epoch:       coordinator.stopCommand.Epoch,
		Phase:       coordinator.stopPhase,
		Result:      result,
		Required:    required,
		Confirmed:   confirmed,
		Outstanding: outstanding,
		Stale:       append([]StaleReceipt(nil), coordinator.staleReceipts...),
		Deadline:    coordinator.stopDeadline,
	}
	status.CanSplice = coordinator.spliceAllowedLocked()
	return status
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
