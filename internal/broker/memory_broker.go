package broker

import (
	"container/heap"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/nitinstp23/go-task-queue/internal/models"
)

type Broker interface {
	Enqueue(req models.EnqueueRequest) (*models.Task, error)
	Dequeue() (*models.Task, bool)
	Acknowledge(id string, result string, err error) error
	GetTask(id string) (*models.Task, error)
	ListTasks(status string) ([]*models.Task, error)
	Stats() models.QueueStats
}

type priorityItem struct {
	task     *models.Task
	priority int
	index    int
}

type priorityQueue []*priorityItem

func (pq priorityQueue) Len() int           { return len(pq) }
func (pq priorityQueue) Less(i, j int) bool { return pq[i].priority > pq[j].priority }
func (pq priorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}
func (pq *priorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*priorityItem)
	item.index = n
	*pq = append(*pq, item)
}
func (pq *priorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*pq = old[:n-1]
	return item
}

type MemoryBroker struct {
	mu        sync.Mutex
	pq        priorityQueue
	tasks     map[string]*models.Task
	enqueued  atomic.Int64
	completed atomic.Int64
	failed    atomic.Int64
}

func NewMemoryBroker() *MemoryBroker {
	mb := &MemoryBroker{tasks: make(map[string]*models.Task)}
	heap.Init(&mb.pq)
	return mb
}

func (mb *MemoryBroker) Enqueue(req models.EnqueueRequest) (*models.Task, error) {
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
	mb.mu.Lock()
	mb.tasks[task.ID] = task
	heap.Push(&mb.pq, &priorityItem{task: task, priority: task.Priority})
	mb.mu.Unlock()
	mb.enqueued.Add(1)
	return task, nil
}

func (mb *MemoryBroker) Dequeue() (*models.Task, bool) {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	for mb.pq.Len() > 0 {
		item := heap.Pop(&mb.pq).(*priorityItem)
		task := item.task
		if task.Status != models.StatusPending {
			continue
		}
		task.Status = models.StatusProcessing
		task.UpdatedAt = time.Now()
		return task, true
	}
	return nil, false
}

func (mb *MemoryBroker) Acknowledge(id string, result string, taskErr error) error {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	task, ok := mb.tasks[id]
	if !ok {
		return errors.New("task not found: " + id)
	}
	now := time.Now()
	task.UpdatedAt = now
	if taskErr != nil {
		task.Retries++
		task.Error = taskErr.Error()
		if task.Retries >= task.MaxRetries {
			task.Status = models.StatusFailed
			mb.failed.Add(1)
		} else {
			task.Status = models.StatusPending
			heap.Push(&mb.pq, &priorityItem{task: task, priority: task.Priority - 1})
		}
	} else {
		task.Status = models.StatusCompleted
		task.Result = result
		task.CompletedAt = &now
		mb.completed.Add(1)
	}
	return nil
}

func (mb *MemoryBroker) GetTask(id string) (*models.Task, error) {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	task, ok := mb.tasks[id]
	if !ok {
		return nil, errors.New("task not found: " + id)
	}
	c := *task
	return &c, nil
}

func (mb *MemoryBroker) ListTasks(status string) ([]*models.Task, error) {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	result := make([]*models.Task, 0, len(mb.tasks))
	for _, task := range mb.tasks {
		if status == "" || string(task.Status) == status {
			c := *task
			result = append(result, &c)
		}
	}
	return result, nil
}

func (mb *MemoryBroker) Stats() models.QueueStats {
	mb.mu.Lock()
	depth := mb.pq.Len()
	mb.mu.Unlock()
	return models.QueueStats{
		TotalEnqueued:  mb.enqueued.Load(),
		TotalCompleted: mb.completed.Load(),
		TotalFailed:    mb.failed.Load(),
		QueueDepth:     depth,
	}
}
