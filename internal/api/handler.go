package api

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/nitinstp23/go-task-queue/internal/broker"
	"github.com/nitinstp23/go-task-queue/internal/models"
	"github.com/nitinstp23/go-task-queue/internal/worker"
)

type Handler struct {
	broker broker.Broker
	pool   *worker.Pool
}

func NewHandler(b broker.Broker, p *worker.Pool) *Handler { return &Handler{broker: b, pool: p} }

func (h *Handler) RegisterRoutes(r *mux.Router) {
	r.HandleFunc("/tasks", h.EnqueueTask).Methods(http.MethodPost)
	r.HandleFunc("/tasks/{id}", h.GetTask).Methods(http.MethodGet)
	r.HandleFunc("/stats", h.GetStats).Methods(http.MethodGet)
	r.HandleFunc("/healthz", h.Health).Methods(http.MethodGet)
}

func (h *Handler) EnqueueTask(w http.ResponseWriter, r *http.Request) {
	var req models.EnqueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error()); return
	}
	task, err := h.broker.Enqueue(req)
	if err != nil {
		writeError(w, http.StatusUnprocessableEntity, err.Error()); return
	}
	writeJSON(w, http.StatusAccepted, task)
}

func (h *Handler) GetTask(w http.ResponseWriter, r *http.Request) {
	task, err := h.broker.GetTask(mux.Vars(r)["id"])
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error()); return
	}
	writeJSON(w, http.StatusOK, task)
}

func (h *Handler) GetStats(w http.ResponseWriter, r *http.Request) {
	stats := h.broker.Stats()
	stats.ActiveWorkers = h.pool.ActiveWorkers()
	writeJSON(w, http.StatusOK, stats)
}

func (h *Handler) Health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
