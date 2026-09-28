package service

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/wyw14/solo-038/internal/dryer"
	"github.com/wyw14/solo-038/internal/folder"
	"github.com/wyw14/solo-038/internal/ink"
	"github.com/wyw14/solo-038/internal/job"
	"github.com/wyw14/solo-038/internal/journal"
	"github.com/wyw14/solo-038/internal/model"
	"github.com/wyw14/solo-038/internal/press"
	registerpkg "github.com/wyw14/solo-038/internal/register"
	"github.com/wyw14/solo-038/internal/sensor"
	"github.com/wyw14/solo-038/internal/splice"
	"github.com/wyw14/solo-038/internal/unwind"
	"github.com/wyw14/solo-038/internal/web"
)

var commandKinds = map[string]model.CommandKind{
	"stop_press":   model.CommandStop,
	"feed_web":     model.CommandFeed,
	"start_dryer":  model.CommandHeat,
	"adjust_phase": model.CommandPhase,
	"start_splice": model.CommandSplice,
	"switch_ink":   model.CommandInkRoute,
}

func (runtime *Runtime) ExecuteOperation(ctx context.Context, kind string) error {
	current, ok := runtime.jobs.Current()
	if !ok {
		return fmt.Errorf("there is no active job")
	}
	if _, err := model.ParseJobID(string(current.ID)); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	switch kind {
	case "load_roll":
		return runtime.loadRollOperation(current)
	case "feed_web":
		return runtime.feedWebOperation(ctx, current)
	case "stop_press":
		return runtime.stopPressOperation(current)
	case "adjust_phase":
		return runtime.registerOperation(current)
	case "switch_ink":
		return runtime.switchInkOperation(false)
	case "ink_fault":
		return runtime.switchInkOperation(true)
	case "start_dryer":
		return runtime.startDryerOperation(ctx, current)
	case "release_interval":
		return runtime.releaseOperation(current)
	case "finish_output":
		return runtime.finishOutputOperation(current)
	case "start_splice":
		return runtime.spliceOperation(ctx, current)
	case "recover":
		return runtime.recoveryOperation(current)
	case "diagnostics":
		return runtime.diagnosticsOperation(ctx, current)
	default:
		return fmt.Errorf("unsupported operation kind %q", kind)
	}
}

func (runtime *Runtime) loadRollOperation(current model.Job) error {
	now := time.Now()
	updated := current.ChangeRoll(now)
	roll := unwind.Roll{ID: string(model.NewOperationID()), WidthMM: 1020, Diameter: 900, LoadedAt: now, ExpiresAt: now.Add(24 * time.Hour)}
	generation, err := runtime.loader.Load(roll)
	if err != nil {
		return err
	}
	updated.RollGeneration = generation
	if err := runtime.jobs.Put(updated); err != nil {
		return err
	}
	return runtime.webControl.Prepare(string(updated.ID), updated.Generation, updated.RollGeneration)
}

func (runtime *Runtime) feedWebOperation(ctx context.Context, current model.Job) error {
	now := time.Now()
	forceA := sensor.Reading{ID: string(model.NewOperationID()), Kind: sensor.ReadingForce, Source: "load-cell-a", Origin: "load-cell-a", Value: 90, Unit: "N", ObservedAt: now}
	forceB := sensor.Reading{ID: string(model.NewOperationID()), Kind: sensor.ReadingForce, Source: "load-cell-b", Origin: "load-cell-b", Value: 91, Unit: "N", ObservedAt: now}
	tension, err := sensor.DeriveTension(forceA, 10, string(model.NewOperationID()))
	if err != nil {
		return err
	}
	speed := sensor.Reading{ID: string(model.NewOperationID()), Kind: sensor.ReadingSpeed, Source: "traction-encoder", Origin: "traction-encoder", Value: 8, Unit: "m/s", ObservedAt: now}
	runtime.sensors.PutReading(forceA)
	runtime.sensors.PutReading(forceB)
	runtime.sensors.PutReading(tension)
	runtime.sensors.PutReading(speed)
	readings := runtime.sensors.CurrentReadings(now, time.Second)
	permit, err := runtime.webControl.Permit([]sensor.Reading{forceA, forceB}, runtime.config.RegisterTolerance, now)
	if err != nil {
		return err
	}
	status, err := unwind.EvaluateTension([]sensor.Reading{forceA, forceB}, 2)
	if err != nil {
		return err
	}
	_, rollGeneration, _, _ := runtime.unwind.Snapshot()
	if rollGeneration == 0 {
		rollGeneration = current.RollGeneration
	}
	if err := unwind.ApplyFeed(runtime.unwind, string(current.ID), rollGeneration, 8, status); err != nil {
		return err
	}
	if err := runtime.webControl.Advance(string(current.ID), 1, permit, now); err != nil {
		return err
	}
	loopCtx, cancel := context.WithCancel(ctx)
	result := web.RunControlLoop(loopCtx, time.Millisecond, 8, func(float64) {})
	cancel()
	if _, err := web.AwaitWorker(context.Background(), result); err != nil {
		return err
	}
	return runtime.verifyPath(readings)
}

func (runtime *Runtime) verifyPath(readings []sensor.Reading) error {
	path, err := web.NewPath([]web.Segment{{Name: "unwind", Length: 4, Tension: 8, Available: true}, {Name: "press", Length: 12, Tension: 8, Available: true}, {Name: "dryer", Length: 18, Tension: 7, Available: true}})
	if err != nil {
		return err
	}
	if err := path.Update("press", 8.2, true); err != nil {
		return err
	}
	if len(path.Segments()) < 3 || len(readings) < 2 {
		return fmt.Errorf("web path diagnostics are incomplete")
	}
	return nil
}

func (runtime *Runtime) stopPressOperation(current model.Job) error {
	now := time.Now()
	command := runtime.pressControl.BeginStop(current, uint64(now.UnixNano()), now)
	deadline := runtime.pressControl.StopDeadline()
	// Every unit gets the stop command and its own receipt slot. A unit
	// that never reports zero speed stays pending; the final status
	// settles as timed_out and the splice stays locked.
	var failures []string
	for _, unit := range runtime.pressUnits.Names() {
		if err := runtime.pressUnits.SetSpeed(unit, 0); err != nil {
			failures = append(failures, unit)
			continue
		}
		unitState, err := runtime.awaitZeroSpeed(unit, deadline)
		if err != nil {
			failures = append(failures, unit)
			continue
		}
		ack := model.Acknowledgement{Unit: unit, Operation: command.Operation, Epoch: command.Epoch, Value: unitState.Speed, At: time.Now().UTC()}
		if err := runtime.pressControl.AcknowledgeStop(ack); err != nil {
			failures = append(failures, unit)
		}
	}
	status := runtime.pressControl.StopStatus(time.Now())
	if !status.Complete() {
		message := fmt.Sprintf("press stop %s: units %s have not confirmed zero speed", status.Outcome, strings.Join(status.Pending, ", "))
		runtime.RecordIncident("warning", "press", message)
		return fmt.Errorf("%s", message)
	}
	if len(failures) > 0 {
		return fmt.Errorf("press stop receipts failed for units %s", strings.Join(failures, ", "))
	}
	return job.RequireSpliceReady(current, true, true, true)
}

// awaitZeroSpeed waits for one press unit to report zero speed before the
// stop deadline. A unit that never reports zero keeps its receipt pending,
// so the stop command settles as timed out and the splice stays locked.
func (runtime *Runtime) awaitZeroSpeed(unit string, deadline time.Time) (press.Unit, error) {
	for {
		unitState, ok := runtime.pressUnits.Unit(unit)
		if !ok {
			return press.Unit{}, fmt.Errorf("press unit %s is unknown", unit)
		}
		if unitState.ZeroSensor {
			return unitState, nil
		}
		if !time.Now().Before(deadline) {
			return press.Unit{}, fmt.Errorf("press unit %s did not reach zero speed before the stop deadline", unit)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (runtime *Runtime) registerOperation(current model.Job) error {
	now := time.Now()
	plateGeneration, err := runtime.pressControl.ChangePlate(string(model.NewOperationID()))
	if err != nil {
		return err
	}
	current.PlateGeneration = plateGeneration
	if err := runtime.register.Start(current.Generation, plateGeneration, 0, now); err != nil {
		return err
	}
	frame := sensor.CameraFrame{ID: string(model.NewOperationID()), Camera: "register-camera", JobGeneration: current.Generation, PlateGeneration: plateGeneration, WebMeter: 1, Offset: 0.04, CapturedAt: now, ReceivedAt: now.Add(time.Millisecond)}
	runtime.sensors.PutFrame(frame)
	if err := runtime.registerControl.AcceptFrame(frame, runtime.config.RegisterGap); err != nil {
		return err
	}
	receiverState := registerpkg.NewState()
	if err := receiverState.Start(current.Generation, plateGeneration, 0, now); err != nil {
		return err
	}
	receiver := registerpkg.NewReceiver(receiverState, runtime.config.RegisterGap)
	if err := receiver.Receive(frame); err != nil {
		return err
	}
	if frame.DeliveryDelay() < 0 || runtime.sensors.FrameCount() == 0 {
		return fmt.Errorf("register camera diagnostics failed")
	}
	if err := runtime.pressControl.RecordRegister(plateGeneration, frame.Offset); err != nil {
		return err
	}
	if !runtime.pressControl.RegisterStable(0.1, 1) || !runtime.registerControl.Stable(1) {
		return fmt.Errorf("register evidence did not stabilize")
	}
	_, err = press.ApplyRegisterFrame(runtime.phase, "cyan", frame, current.Generation, plateGeneration)
	return err
}

func (runtime *Runtime) switchInkOperation(withFault bool) error {
	active := runtime.inkControl.Active()
	if active == "" {
		if err := runtime.inkControl.Activate("cyan"); err != nil {
			return err
		}
		active = "cyan"
	}
	if withFault {
		circuit := ink.NewCircuit(runtime.ink)
		circuit.FailDrain(active, fmt.Errorf("drain isolation signal failed"))
		runtime.RecordIncident("warning", "ink", "drain isolation signal failed")
		return nil
	}
	next := "magenta"
	if active == next {
		next = "cyan"
	}
	if _, err := runtime.inkControl.SwitchColor(next); err != nil {
		return err
	}
	if err := runtime.press.SetColor(next); err != nil {
		return err
	}
	colors := runtime.ink.Colors()
	if len(colors) < 2 || !ink.ActiveCircuitReady(runtime.ink, next) {
		return fmt.Errorf("ink circuit did not become ready")
	}
	return ink.RequireReady(runtime.ink, active)
}

func (runtime *Runtime) startDryerOperation(ctx context.Context, current model.Job) error {
	profile := dryer.Profile{Name: "production", Zones: []dryer.Zone{{Name: "entry", Temperature: 80, Airflow: 20}, {Name: "exit", Temperature: 100, Airflow: 24}}}
	if profile.MaximumTemperature() <= 0 || profile.AverageAirflow() <= 0 {
		return fmt.Errorf("dryer profile is invalid")
	}
	if err := runtime.dryerControl.Startup(nil, true); err != nil {
		return err
	}
	if err := runtime.jobControl.StartDryerRamp(ctx, current, profile.MaximumTemperature(), 10*time.Millisecond); err != nil {
		return err
	}
	_ = runtime.jobControl.DryerTemperature()
	return nil
}

func (runtime *Runtime) releaseOperation(current model.Job) error {
	now := time.Now()
	window := runtime.register.Snapshot()
	interval := model.Interval{JobID: current.ID, JobGeneration: current.Generation, RollGeneration: current.RollGeneration, StartMeter: window.StartMeter, EndMeter: window.LastMeter, OpenedAt: window.StartedAt, ClosedAt: now}
	proof, err := dryer.BuildProof(current, interval, 100, 24, now)
	if err != nil {
		return err
	}
	if !proof.Covers(interval) {
		return fmt.Errorf("dryer proof does not cover interval")
	}
	proofs := model.ProofSet{Interval: interval, Proofs: []model.Proof{proof.Proof}}
	_ = proofs.Sources()
	if err := registerpkg.ReleaseWithProofs(runtime.registerControl, current, model.NewOperationID(), proofs.Proofs, true, now); err != nil {
		return err
	}
	if !runtime.registerControl.CanAdvance(current) || runtime.registerControl.LastError() != nil {
		return fmt.Errorf("released interval cannot advance")
	}
	_, err = journal.PersistFence(runtime.journal, current, model.NewOperationID(), string(current.ID), now)
	return err
}

func (runtime *Runtime) finishOutputOperation(current model.Job) error {
	if err := runtime.folderControl.Prepare(8, 630); err != nil {
		return err
	}
	if err := runtime.folderControl.Finish(); err != nil {
		return err
	}
	output, err := folder.BuildOutput(current, 500, time.Now())
	if err != nil {
		return err
	}
	if !output.BelongsTo(current) {
		return fmt.Errorf("folder output generation mismatch")
	}
	return nil
}

func (runtime *Runtime) spliceOperation(ctx context.Context, current model.Job) error {
	operation, err := runtime.spliceControl.Prepare(string(current.ID), 20, nil, time.Now())
	if err != nil {
		return err
	}
	if err := runtime.spliceControl.StartWorker(ctx, operation, 5*time.Millisecond); err != nil {
		return err
	}
	time.Sleep(6 * time.Millisecond)
	_ = runtime.spliceControl.Activity()
	if runtime.spliceControl.FestoonAvailable() < 0 || len(runtime.festoon.Reservations()) == 0 {
		return fmt.Errorf("splice festoon reservation is invalid")
	}
	if err := runtime.spliceControl.Cancel(operation, time.Now()); err != nil {
		return err
	}
	standalone := splice.NewState()
	standaloneOperation := model.NewOperationID()
	if err := standalone.Begin(standaloneOperation, model.NewLeaseID(), time.Now()); err != nil {
		return err
	}
	if _, err := splice.CancelOutcome(standalone, standaloneOperation, "operator cancel", time.Now()); err != nil {
		return err
	}
	return nil
}

func (runtime *Runtime) recoveryOperation(current model.Job) error {
	if err := runtime.SaveSnapshot(); err != nil {
		return err
	}
	if _, err := runtimeLoadSnapshot(runtime.config.DataDir); err != nil {
		return err
	}
	recovered := journal.RecoveredState{Job: current, Owner: string(current.ID)}
	decision, err := job.DecideRecovery(recovered)
	if err != nil {
		return err
	}
	recoveredJobs := job.NewState()
	if err := job.RestoreState(recoveredJobs, decision); err != nil {
		return err
	}
	if _, err := runtime.RecoverFeed(recovered, 5); err != nil {
		return err
	}
	_ = web.Capture(runtime.web)
	return nil
}

func (runtime *Runtime) diagnosticsOperation(ctx context.Context, current model.Job) error {
	if len(runtime.jobs.IDs()) == 0 {
		return fmt.Errorf("job diagnostics found no jobs")
	}
	if _, err := job.CurrentGeneration(runtime.jobs); err != nil {
		return err
	}
	if _, ok := commandKinds["stop_press"]; !ok {
		return fmt.Errorf("command registry is incomplete")
	}
	if runtime.registerControl.LastError() != nil {
		runtime.RecordIncident("error", "register", runtime.registerControl.LastError().Error())
	}
	if runtime.unwind.StopFeed(string(current.ID)) {
		return runtime.feedWebOperation(ctx, current)
	}
	store := journal.NewStore(filepath.Join(runtime.config.DataDir, "diagnostic-events.jsonl"))
	store.FailNext(fmt.Errorf("diagnostic write fault"))
	if _, err := registerpkg.RebuildFromJournal(store); err != nil {
		return err
	}
	localPress := press.NewState()
	if err := press.RestorePhases(localPress, "diagnostic-plate", 1, map[string]float64{"cyan": 0}); err != nil {
		return err
	}
	return nil
}
