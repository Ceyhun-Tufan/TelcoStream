package api

import (
	"encoding/json"
	"net/http"
	"telco-stream/internal/domain"
	"telco-stream/internal/kafka"
)

type CDRHandler struct {
	producer kafka.Producer
}

func NewCDRHandler(producer kafka.Producer) *CDRHandler {
	return &CDRHandler{
		producer: producer,
	}
}

func (h *CDRHandler) IngestCDR(w http.ResponseWriter, r *http.Request) {

	var event domain.CDREvent

	// json valide etme / serialize deserialize mantigi
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		http.Error(w, "Invalid json body: "+err.Error(), http.StatusBadRequest)
		return
	}

	// validation
	if err := event.Validate(); err != nil {
		http.Error(w, "Validation failed: "+err.Error(), http.StatusBadRequest)
		return
	}

	// kafkada sorun var mi
	if err := h.producer.PublishCDR(r.Context(), event); err != nil {
		http.Error(w, "failed to publish event: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"status":"accepted","event_id":"` + event.EventID + `"}`))

}
