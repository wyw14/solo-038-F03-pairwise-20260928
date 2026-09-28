package service

import "time"

// PressStopUnit is one printing unit's receipt state for the current stop
// command: pending, confirmed, late, or timeout.
type PressStopUnit struct {
	Unit   string    `json:"unit"`
	Status string    `json:"status"`
	At     time.Time `json:"at,omitempty"`
}

// PressStopStatus is the operator view of the most recent stop command:
// which units have confirmed zero speed, which are still outstanding, and
// how the attempt settled (in_progress, complete, timed_out).
type PressStopStatus struct {
	Operation   string          `json:"operation"`
	Epoch       uint64          `json:"epoch"`
	IssuedAt    time.Time       `json:"issued_at"`
	Deadline    time.Time       `json:"deadline"`
	Outcome     string          `json:"outcome"`
	CanSplice   bool            `json:"can_splice"`
	Units       []PressStopUnit `json:"units"`
	Confirmed   []string        `json:"confirmed"`
	Pending     []string        `json:"pending"`
	CompletedAt time.Time       `json:"completed_at,omitempty"`
	SettledAt   time.Time       `json:"settled_at,omitempty"`
}

// PressStop returns the receipt status of the most recent stop command. The
// second return value is false when no stop command has been issued yet.
func (runtime *Runtime) PressStop() (PressStopStatus, bool) {
	now := time.Now()
	status := runtime.pressControl.StopStatus(now)
	if !status.Operation.Valid() {
		return PressStopStatus{}, false
	}
	view := PressStopStatus{
		Operation:   string(status.Operation),
		Epoch:       status.Epoch,
		IssuedAt:    status.IssuedAt,
		Deadline:    status.Deadline,
		Outcome:     status.Outcome,
		CanSplice:   runtime.pressControl.CanSplice(),
		Units:       make([]PressStopUnit, 0, len(status.Units)),
		Confirmed:   status.Confirmed,
		Pending:     status.Pending,
		CompletedAt: status.CompletedAt,
		SettledAt:   status.SettledAt,
	}
	for _, unit := range status.Units {
		view.Units = append(view.Units, PressStopUnit{Unit: unit.Unit, Status: unit.Status, At: unit.At})
	}
	return view, true
}
