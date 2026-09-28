package service

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/wyw14/solo-038/internal/model"
	"github.com/wyw14/solo-038/internal/unwind"
)

type Operation struct {
	ID        model.OperationID `json:"id"`
	Kind      string            `json:"kind"`
	JobID     model.JobID       `json:"job_id"`
	Status    string            `json:"status"`
	CreatedAt time.Time         `json:"created_at"`
}

func (runtime *Runtime) CreateOperation(ctx context.Context, kind string) (Operation, error) {
	if kind == "" {
		return Operation{}, fmt.Errorf("operation kind is required")
	}
	select {
	case <-ctx.Done():
		return Operation{}, ctx.Err()
	default:
	}
	jobValue, ok := runtime.jobs.Current()
	if !ok {
		return Operation{}, fmt.Errorf("there is no active job")
	}
	if err := runtime.ExecuteOperation(ctx, kind); err != nil {
		return Operation{}, err
	}
	return Operation{ID: model.NewOperationID(), Kind: kind, JobID: jobValue.ID, Status: "accepted", CreatedAt: time.Now().UTC()}, nil
}

func (runtime *Runtime) Equipment() []map[string]any {
	roll, generation, feedOwner, feedSpeed := runtime.unwind.Snapshot()
	plate, plateGeneration, color, phases := runtime.press.Snapshot()
	ready, folderSpeed, cutLength, fault, lots := runtime.folder.Snapshot()
	values := []map[string]any{
		{"name": "unwind", "roll": roll, "generation": generation, "owner": feedOwner, "speed": feedSpeed},
		{"name": "press", "plate": plate, "generation": plateGeneration, "color": color, "phases": phases, "stop": runtime.PressStopStatus()},
		{"name": "dryer", "state": runtime.dryerControlStatus()},
		{"name": "folder", "ready": ready, "speed": folderSpeed, "cut_length": cutLength, "fault": fault, "lots": lots},
	}
	sort.Slice(values, func(left, right int) bool { return values[left]["name"].(string) < values[right]["name"].(string) })
	return values
}

func (runtime *Runtime) dryerControlStatus() map[string]any {
	status, errors := runtime.dryerControl.Status()
	jobID, generation, target, current, profile, active := runtime.dryer.Snapshot()
	return map[string]any{"startup": status, "errors": errors, "job_id": jobID, "generation": generation, "target": target, "current": current, "profile": profile, "active": active}
}

func (runtime *Runtime) LoadRoll(roll unwind.Roll) (uint64, error) {
	return runtime.loader.Load(roll)
}
