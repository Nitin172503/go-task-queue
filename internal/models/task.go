package models

import "time"

type TaskStatus string
const (
	StatusPending    TaskStatus = "pending"
	StatusProcessing TaskStatus = "processing"
	StatusCompleted  TaskStatus = "completed"
	StatusFailed     TaskStatus = "failed"
)

type TaskType string
const (
	TaskTypeEmail  TaskType = "email"
	TaskTypeResize TaskType = "image_resize"
	TaskTypeReport TaskType = "generate_report"
	TaskTypeNotify TaskType = "notification"
)

type Task struct {
	ID          string            `json:"id"`
	Type        TaskType          `json:"type"`
	Payload     map[string]string `json:"payload"`
	Status      TaskStatus        `json:"status"`
	Priority    int               `json:"priority"`
	Retries     int               `json:"retries"`
	MaxRetries  int               `json:"max_retries"`
	CreatedAt   time.Time         `json:"created_at"`
	UpdatedAt   time.Time         `json:"updated_at"`
	CompletedAt *time.Time        `json:"completed_at,omitempty"`
	Error       string            `json:"error,omitempty"`
	Result      string            `json:"result,omitempty"`
}

type EnqueueRequest struct {
	Type       TaskType          `json:"type"`
	Payload    map[string]string `json:"payload"`
	Priority   int               `json:"priority"`
	MaxRetries int               `json:"max_retries"`
}

type QueueStats struct {
	TotalEnqueued  int64 `json:"total_enqueued"`
	TotalCompleted int64 `json:"total_completed"`
	TotalFailed    int64 `json:"total_failed"`
	PendingCount   int   `json:"pending_count"`
	ActiveWorkers  int   `json:"active_workers"`
	QueueDepth     int   `json:"queue_depth"`
}
