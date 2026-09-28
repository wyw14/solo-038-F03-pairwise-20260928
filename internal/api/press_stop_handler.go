package api

import (
	"net/http"
	"time"

	"github.com/wyw14/solo-038/internal/service"
)

type stopAcknowledgementRequest struct {
	Unit  string    `json:"unit"`
	Value float64   `json:"value"`
	At    time.Time `json:"at"`
}

// pressStop reports the current stop command, the units that have not
// confirmed zero speed, and the result of this stop.
func (handler *Handler) pressStop(writer http.ResponseWriter, request *http.Request) {
	writeJSON(writer, http.StatusOK, handler.runtime.PressStopStatus())
}

// acknowledgePressStop files one printing unit's zero-speed receipt for the
// active stop command.
func (handler *Handler) acknowledgePressStop(writer http.ResponseWriter, request *http.Request) {
	var input stopAcknowledgementRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err)
		return
	}
	status, err := handler.runtime.ReceiveStopAcknowledgement(service.StopAcknowledgement{Unit: input.Unit, Value: input.Value, At: input.At})
	if err != nil {
		writeError(writer, http.StatusConflict, err)
		return
	}
	writeJSON(writer, http.StatusAccepted, status)
}
