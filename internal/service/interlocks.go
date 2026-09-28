package service

import (
	"sort"
	"strings"
	"time"
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
	stopStatus, stopIssued := runtime.PressStop()
	stopDetail := "no stop command has been issued"
	if stopIssued {
		switch {
		case stopStatus.CanSplice:
			stopDetail = "all press units confirmed zero speed for the current stop epoch"
		case len(stopStatus.Pending) > 0:
			stopDetail = "waiting on zero-speed receipts from: " + strings.Join(stopStatus.Pending, ", ") + " (outcome " + stopStatus.Outcome + ")"
		default:
			stopDetail = "current stop epoch outcome: " + stopStatus.Outcome
		}
	}
	values := []Interlock{
		{Name: "web_threaded", Satisfied: threaded, Detail: "paper web is fenced and threaded", CheckedAt: now},
		{Name: "dryer_ready", Satisfied: status == "ready", Detail: "dryer damper and profile startup are complete", CheckedAt: now},
		{Name: "folder_ready", Satisfied: ready, Detail: fault, CheckedAt: now},
		{Name: "press_stopped_for_splice", Satisfied: stopStatus.CanSplice, Detail: stopDetail, CheckedAt: now},
	}
	sort.Slice(values, func(left, right int) bool { return values[left].Name < values[right].Name })
	return values
}
