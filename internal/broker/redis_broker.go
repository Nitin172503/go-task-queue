package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/nitinstp23/go-task-queue/internal/models"
)

const (
	redisQueueKey   = "taskqueue:pending"
	redisTaskKey    = "taskqueue:task:"
	redisTaskSetKey = "taskqueue:all"
)

type RedisBroker struct {
	client    *redis.Client
	ctx       context.Context
	enqueued  atomic.Int64
	completed atomic.Int64
	failed    atomic.Int64
}

func NewRedisBroker(addr string) *RedisBroker {
	client := redis.NewClient(&redis.Options{
		Addr:         addr,
		DialTimeout:  3 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	})
	return &RedisBroker{client: client, ctx: context.Background()}
}

func (rb *RedisBroker) Ping() error {
	return rb.client.Ping(rb.ctx).Err()
}

func (rb *RedisBroker) Enqueue(req models.EnqueueRequest) (*models.Task, error) {
	if req.Type == "" {
		return nil, errors.New("task type is required")
	}
	if req.MaxRetries == 0 {
		req.MaxRetries = 3
	}
	task := &models.Task{
		ID:         uuid.NewString(),
		Type:       req.Type,
		Payload:    req.Payload,
		Status:     models.StatusPending,
		Priority:   req.Priority,
		MaxRetries: req.MaxRetries,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}
	data, err := json.Marshal(task)
	if err != nil {
		return nil, fmt.Errorf("marshal task: %w", err)
	}
	pipe := rb.client.Pipeline()
	pipe.Set(rb.ctx, redisTaskKey+task.ID, data, 24*time.Hour)
	pipe.ZAdd(rb.ctx, redisQueueKey, redis.Z{Score: float64(-task.Priority), Member: task.ID})
	pipe.SAdd(rb.ctx, redisTaskSetKey, task.ID)
	if _, err := pipe.Exec(rb.ctx); err != nil {
		return nil, fmt.Errorf("enqueue task: %w", err)
	}
	rb.enqueued.Add(1)
	return task, nil
}

func (rb *RedisBroker) Dequeue() (*models.Task, bool) {
	results, err := rb.client.ZPopMin(rb.ctx, redisQueueKey, 1).Result()
	if err != nil || len(results) == 0 {
		return nil, false
	}
	id := results[0].Member.(string)
	task, err := rb.fetchTask(id)
	if err != nil {
		return nil, false
	}
	task.Status = models.StatusProcessing
	task.UpdatedAt = time.Now()
	if err := rb.saveTask(task); err != nil {
		return nil, false
	}
	return task, true
}

func (rb *RedisBroker) Acknowledge(id string, result string, taskErr error) error {
	task, err := rb.fetchTask(id)
	if err != nil {
		return err
	}
	now := time.Now()
	task.UpdatedAt = now
	if taskErr != nil {
		task.Retries++
		task.Error = taskErr.Error()
		if task.Retries >= task.MaxRetries {
			task.Status = models.StatusFailed
			rb.failed.Add(1)
		} else {
			task.Status = models.StatusPending
			if err := rb.client.ZAdd(rb.ctx, redisQueueKey, redis.Z{
				Score:  float64(-(task.Priority - 1)),
				Member: task.ID,
			}).Err(); err != nil {
				return fmt.Errorf("re-enqueue task: %w", err)
			}
		}
	} else {
		task.Status = models.StatusCompleted
		task.Result = result
		task.CompletedAt = &now
		rb.completed.Add(1)
	}
	return rb.saveTask(task)
}

func (rb *RedisBroker) GetTask(id string) (*models.Task, error) {
	return rb.fetchTask(id)
}

func (rb *RedisBroker) ListTasks(status string) ([]*models.Task, error) {
	ids, err := rb.client.SMembers(rb.ctx, redisTaskSetKey).Result()
	if err != nil {
		return nil, fmt.Errorf("list task IDs: %w", err)
	}
	tasks := make([]*models.Task, 0, len(ids))
	for _, id := range ids {
		task, err := rb.fetchTask(id)
		if err != nil {
			continue
		}
		if status == "" || string(task.Status) == status {
			tasks = append(tasks, task)
		}
	}
	return tasks, nil
}

func (rb *RedisBroker) Stats() models.QueueStats {
	depth, _ := rb.client.ZCard(rb.ctx, redisQueueKey).Result()
	return models.QueueStats{
		TotalEnqueued:  rb.enqueued.Load(),
		TotalCompleted: rb.completed.Load(),
		TotalFailed:    rb.failed.Load(),
		QueueDepth:     int(depth),
	}
}

func (rb *RedisBroker) fetchTask(id string) (*models.Task, error) {
	data, err := rb.client.Get(rb.ctx, redisTaskKey+id).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, fmt.Errorf("task not found: %s", id)
		}
		return nil, fmt.Errorf("fetch task: %w", err)
	}
	var task models.Task
	if err := json.Unmarshal(data, &task); err != nil {
		return nil, fmt.Errorf("unmarshal task: %w", err)
	}
	return &task, nil
}

func (rb *RedisBroker) saveTask(task *models.Task) error {
	data, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("marshal task: %w", err)
	}
	return rb.client.Set(rb.ctx, redisTaskKey+task.ID, data, 24*time.Hour).Err()
}
