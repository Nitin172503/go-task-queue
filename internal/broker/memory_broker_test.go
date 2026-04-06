package broker_test

import (
	"errors"
	"testing"

	"github.com/nitinstp23/go-task-queue/internal/broker"
	"github.com/nitinstp23/go-task-queue/internal/models"
)

func newBroker() *broker.MemoryBroker { return broker.NewMemoryBroker() }

func enqueueTask(t *testing.T, b *broker.MemoryBroker, tt models.TaskType, priority int) *models.Task {
	t.Helper()
	task, err := b.Enqueue(models.EnqueueRequest{Type: tt, Payload: map[string]string{"key": "val"}, Priority: priority, MaxRetries: 2})
	if err != nil { t.Fatalf("enqueue failed: %v", err) }
	return task
}

func TestEnqueue_Basic(t *testing.T) {
	b := newBroker()
	task := enqueueTask(t, b, models.TaskTypeEmail, 5)
	if task.ID == "" { t.Error("expected non-empty ID") }
	if task.Status != models.StatusPending { t.Errorf("expected pending, got %s", task.Status) }
}

func TestEnqueue_MissingType(t *testing.T) {
	b := newBroker()
	_, err := b.Enqueue(models.EnqueueRequest{})
	if err == nil { t.Error("expected error") }
}

func TestDequeue_Priority(t *testing.T) {
	b := newBroker()
	enqueueTask(t, b, models.TaskTypeEmail, 1)
	enqueueTask(t, b, models.TaskTypeReport, 10)
	enqueueTask(t, b, models.TaskTypeNotify, 5)
	first, _ := b.Dequeue()
	if first.Priority != 10 { t.Errorf("expected 10, got %d", first.Priority) }
	second, _ := b.Dequeue()
	if second.Priority != 5 { t.Errorf("expected 5, got %d", second.Priority) }
}

func TestDequeue_Empty(t *testing.T) {
	b := newBroker()
	_, ok := b.Dequeue()
	if ok { t.Error("expected false") }
}

func TestAcknowledge_Success(t *testing.T) {
	b := newBroker()
	task := enqueueTask(t, b, models.TaskTypeEmail, 1)
	b.Dequeue()
	b.Acknowledge(task.ID, "done", nil)
	fetched, _ := b.GetTask(task.ID)
	if fetched.Status != models.StatusCompleted { t.Errorf("expected completed, got %s", fetched.Status) }
}

func TestAcknowledge_Retry(t *testing.T) {
	b := newBroker()
	task := enqueueTask(t, b, models.TaskTypeEmail, 1)
	b.Dequeue()
	b.Acknowledge(task.ID, "", errors.New("err"))
	fetched, _ := b.GetTask(task.ID)
	if fetched.Retries != 1 { t.Errorf("expected 1 retry, got %d", fetched.Retries) }
}

func TestAcknowledge_MaxRetries(t *testing.T) {
	b := newBroker()
	task, _ := b.Enqueue(models.EnqueueRequest{Type: models.TaskTypeEmail, Payload: map[string]string{"to": "x"}, Priority: 1, MaxRetries: 1})
	b.Dequeue()
	b.Acknowledge(task.ID, "", errors.New("fail"))
	b.Dequeue()
	b.Acknowledge(task.ID, "", errors.New("fail"))
	fetched, _ := b.GetTask(task.ID)
	if fetched.Status != models.StatusFailed { t.Errorf("expected failed, got %s", fetched.Status) }
}

func TestGetTask_NotFound(t *testing.T) {
	b := newBroker()
	_, err := b.GetTask("bad-id")
	if err == nil { t.Error("expected error") }
}
