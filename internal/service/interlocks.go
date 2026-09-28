package service

import (
	"sort"
	"strings"
	"time"

	"github.com/wyw14/solo-038/internal/press"
)

type Interlock struct {
	Name      string    `json:"name"`
	Satisfied bool      `json:"satisfied"`
	Detail    string    `json:"detail"`
	CheckedAt time.Time `json:"checked_at"`
}

func (runtime *Runtime) Interlocks() []Interlock {
	now := time.Now().UTC()
	_, _, _, threaded, _ := runtime.web.Snapshot()
	status, _ := runtime.dryerControl.Status()
	ready, _, _, fault, _ := runtime.folder.Snapshot()
	stop := runtime.pressControl.StopStatus(now)
	stopDetail := "no stop command has been issued"
	switch stop.Phase {
	case press.StopAwaiting:
		stopDetail = "waiting for zero-speed receipts from: " + joinUnitNames(stop.Outstanding)
	case press.StopComplete:
		stopDetail = "all printing units confirmed zero speed"
	case press.StopTimedOut:
		stopDetail = "stop timed out; missing zero-speed receipts from: " + joinUnitNames(stop.Outstanding)
	}
	values := []Interlock{
		{Name: "web_threaded", Satisfied: threaded, Detail: "paper web is fenced and threaded", CheckedAt: now},
		{Name: "dryer_ready", Satisfied: status == "ready", Detail: "dryer damper and profile startup are complete", CheckedAt: now},
		{Name: "folder_ready", Satisfied: ready, Detail: fault, CheckedAt: now},
		{Name: "press_stopped_for_splice", Satisfied: stop.CanSplice, Detail: stopDetail, CheckedAt: now},
	}
	sort.Slice(values, func(left, right int) bool { return values[left].Name < values[right].Name })
	return values
}

func joinUnitNames(units []string) string {
	if len(units) == 0 {
		return "none"
	}
	return strings.Join(units, ", ")
}
