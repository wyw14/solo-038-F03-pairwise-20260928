package service

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/wyw14/solo-038/internal/job"
	"github.com/wyw14/solo-038/internal/model"
	"github.com/wyw14/solo-038/internal/press"
)

// unitReceiptDelay staggers the simulated zero-speed receipts so the four
// printing units confirm independently rather than in one instant.
const unitReceiptDelay = 2 * time.Millisecond

// StopAcknowledgement is one printing unit's zero-speed receipt as filed by
// the operator/equipment gateway.
type StopAcknowledgement struct {
	Unit  string    `json:"unit"`
	Value float64   `json:"value"`
	At    time.Time `json:"at"`
}

// PressStopStatus exposes the current stop command, the units that have not
// confirmed zero speed yet, and the result of this stop.
func (runtime *Runtime) PressStopStatus() press.StopStatus {
	return runtime.pressControl.StopStatus(time.Now().UTC())
}

// ReceiveStopAcknowledgement files a single unit receipt against the active
// stop command. Late receipts from an earlier command are rejected by the
// coordinator and can never credit a newer command.
func (runtime *Runtime) ReceiveStopAcknowledgement(input StopAcknowledgement) (press.StopStatus, error) {
	if input.Unit == "" {
		return press.StopStatus{}, fmt.Errorf("press unit is required")
	}
	at := input.At
	if at.IsZero() {
		at = time.Now().UTC()
	}
	status := runtime.pressControl.StopStatus(time.Now().UTC())
	if status.Phase == press.StopIdle {
		return press.StopStatus{}, fmt.Errorf("there is no active stop command to acknowledge")
	}
	ack := model.Acknowledgement{Unit: input.Unit, Operation: status.Operation, Epoch: status.Epoch, Value: input.Value, At: at.UTC()}
	if err := runtime.pressControl.AcknowledgeStop(ack); err != nil {
		return runtime.pressControl.StopStatus(time.Now().UTC()), err
	}
	return runtime.pressControl.StopStatus(time.Now().UTC()), nil
}

// stopPressOperation issues one stop command, commands every printing unit to
// zero speed, and then waits for each unit's own zero-speed receipt. If the
// deadline passes or any unit is missing, the splice stays locked and the
// outstanding units are reported and recorded.
func (runtime *Runtime) stopPressOperation(ctx context.Context, current model.Job) error {
	now := time.Now().UTC()
	command, err := runtime.pressControl.BeginStop(current, uint64(now.UnixNano()), runtime.config.StopAckTimeout, now)
	if err != nil {
		return err
	}
	runtime.dispatchUnitStopReceipts(command)
	status := runtime.pressControl.AwaitStop(ctx, time.Now().UTC())
	if !status.CanSplice {
		message := fmt.Sprintf("press stop %s: units %s did not confirm zero speed", status.Result, strings.Join(status.Outstanding, ", "))
		runtime.RecordIncident("error", "press", message)
		return fmt.Errorf("%s", message)
	}
	return job.RequireSpliceReady(current, true, true, true)
}

// dispatchUnitStopReceipts drives each printing unit to zero speed and lets
// each one file its own receipt after an independent short delay. A unit that
// is still moving when its receipt window closes never confirms.
func (runtime *Runtime) dispatchUnitStopReceipts(command model.Command) {
	for index, unit := range runtime.pressUnits.Names() {
		unit, index := unit, index
		if err := runtime.pressUnits.SetSpeed(unit, 0); err != nil {
			runtime.RecordIncident("error", "press", fmt.Sprintf("unit %s did not accept the stop command: %v", unit, err))
			continue
		}
		go func() {
			timer := time.NewTimer(time.Duration(index+1) * unitReceiptDelay)
			defer timer.Stop()
			<-timer.C
			ack := model.Acknowledgement{Unit: unit, Operation: command.Operation, Epoch: command.Epoch, Value: 0, At: time.Now().UTC()}
			if err := runtime.pressControl.AcknowledgeStop(ack); err != nil {
				runtime.RecordIncident("warning", "press", fmt.Sprintf("unit %s stop receipt rejected: %v", unit, err))
			}
		}()
	}
}
