package service

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/wyw14/solo-038/internal/dryer"
	"github.com/wyw14/solo-038/internal/folder"
	"github.com/wyw14/solo-038/internal/ink"
	"github.com/wyw14/solo-038/internal/job"
	"github.com/wyw14/solo-038/internal/journal"
	"github.com/wyw14/solo-038/internal/model"
	"github.com/wyw14/solo-038/internal/press"
	"github.com/wyw14/solo-038/internal/register"
	"github.com/wyw14/solo-038/internal/sensor"
	"github.com/wyw14/solo-038/internal/splice"
	"github.com/wyw14/solo-038/internal/unwind"
	"github.com/wyw14/solo-038/internal/web"
)

type Config struct {
	DataDir           string
	FestoonCapacity   float64
	RegisterGap       time.Duration
	RegisterTolerance float64
	StopAckTimeout    time.Duration
}

// defaultStopAckTimeout bounds how long a stop command waits for the four
// printing units' zero-speed receipts before the splice stays locked.
const defaultStopAckTimeout = 2 * time.Second

type Runtime struct {
	mu              sync.RWMutex
	startedAt       time.Time
	config          Config
	journal         *journal.Store
	jobs            *job.State
	jobControl      *job.Coordinator
	sensors         *sensor.State
	unwind          *unwind.State
	loader          *unwind.Loader
	festoon         *unwind.Festoon
	web             *web.State
	webControl      *web.Coordinator
	press           *press.State
	pressUnits      *press.Controller
	pressControl    *press.Coordinator
	phase           *press.PhaseController
	ink             *ink.State
	inkControl      *ink.Coordinator
	register        *register.State
	registerControl *register.Coordinator
	dryer           *dryer.State
	dryerControl    *dryer.Coordinator
	folder          *folder.State
	folderControl   *folder.Controller
	splice          *splice.State
	spliceControl   *splice.Coordinator
	incidents       []Incident
}

func NewRuntime(config Config) (*Runtime, error) {
	if config.DataDir == "" || config.FestoonCapacity <= 0 || config.RegisterGap <= 0 || config.RegisterTolerance <= 0 {
		return nil, fmt.Errorf("runtime configuration is incomplete")
	}
	if config.StopAckTimeout <= 0 {
		config.StopAckTimeout = defaultStopAckTimeout
	}
	path, err := web.NewPath([]web.Segment{{Name: "unwind", Length: 4, Tension: 8, Available: true}, {Name: "press", Length: 12, Tension: 8, Available: true}, {Name: "dryer", Length: 18, Tension: 7, Available: true}, {Name: "folder", Length: 5, Tension: 6, Available: true}})
	if err != nil {
		return nil, err
	}
	journalStore := journal.NewStore(filepath.Join(config.DataDir, "events.jsonl"))
	dryerState := dryer.NewState()
	jobState := job.NewState()
	unwindState := unwind.NewState()
	webState := web.NewState()
	pressState := press.NewState()
	pressController, err := press.NewController("cyan", "magenta", "yellow", "black")
	if err != nil {
		return nil, err
	}
	inkState := ink.NewState("cyan", "magenta", "yellow", "black")
	registerState := register.NewState()
	folderState := folder.NewState()
	spliceState := splice.NewState()
	festoon := unwind.NewFestoon(config.FestoonCapacity)
	runtime := &Runtime{startedAt: time.Now().UTC(), config: config, journal: journalStore, jobs: jobState, jobControl: job.NewCoordinator(jobState, dryerState), sensors: sensor.NewState(), unwind: unwindState, loader: unwind.NewLoader(unwindState), festoon: festoon, web: webState, webControl: web.NewCoordinator(webState, path), press: pressState, pressUnits: pressController, pressControl: press.NewCoordinator(pressController, pressState), phase: press.NewPhaseController(pressState, 2.5), ink: inkState, register: registerState, registerControl: register.NewCoordinator(registerState, journalStore), dryer: dryerState, dryerControl: dryer.NewCoordinator(dryerState), folder: folderState, folderControl: folder.NewController(folderState), splice: spliceState}
	runtime.inkControl = ink.NewCoordinator(ink.NewCircuit(inkState))
	runtime.spliceControl = splice.NewCoordinator(spliceState, festoon, splice.NewLeaseManager())
	return runtime, nil
}

func (runtime *Runtime) Start(ctx context.Context) error {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if _, ok := runtime.jobs.Current(); !ok {
		created := runtime.jobs.Create(time.Now())
		created.RollGeneration = 1
		created.PlateGeneration = 1
		if err := runtime.jobs.Put(created); err != nil {
			return err
		}
	}
	return nil
}

func (runtime *Runtime) Healthy() bool {
	runtime.mu.RLock()
	defer runtime.mu.RUnlock()
	return !runtime.startedAt.IsZero() && runtime.journal != nil && runtime.jobs != nil
}

func (runtime *Runtime) CurrentJob() (model.Job, bool) {
	return runtime.jobs.Current()
}
