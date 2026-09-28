package api

import "net/http"

func (handler *Handler) pressStop(writer http.ResponseWriter, request *http.Request) {
	status, ok := handler.runtime.PressStop()
	if !ok {
		writeJSON(writer, http.StatusNotFound, map[string]any{"error": "no stop command has been issued"})
		return
	}
	writeJSON(writer, http.StatusOK, status)
}
