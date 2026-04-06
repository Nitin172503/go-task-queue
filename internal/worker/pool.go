package worker

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nitinstp23/go-task-queue/internal/broker"
	"github.com/nitinstp23/go-task-queue/internal/models"
)

type HandlerFunc func(task *models.Task) (string, error)

type Pool struct {
	broker       broker.Broker
	handlers     map[models.TaskType]HandlerFunc
	workerCount  int
	activeCount  atomic.Int32
	pollInterval time.Duration
	wg           sync.WaitGroup
}

func NewPool(b broker.Broker, workerCount int, pollInterval time.Duration) *Pool {
	p := &Pool{broker: b, handlers: make(map[models.TaskType]HandlerFunc), workerCount: workerCount, pollInterval: pollInterval}
	p.registerDefaultHandlers()
	return p
}

func (p *Pool) Start(ctx context.Context) {
	log.Printf("[pool] starting %d workers", p.workerCount)
	for i := 0; i < p.workerCount; i++ {
		p.wg.Add(1)
		go p.runWorker(ctx, i)
	}
	p.wg.Wait()
}

func (p *Pool) ActiveWorkers() int { return int(p.activeCount.Load()) }

func (p *Pool) runWorker(ctx context.Context, id int) {
	defer p.wg.Done()
	for {
		select {
		case <-ctx.Done():
			log.Printf("[worker-%d] shutting down", id)
			return
		default:
		}
		task, ok := p.broker.Dequeue()
		if !ok {
			select {
			case <-ctx.Done():
				return
			case <-time.After(p.pollInterval):
			}
			continue
		}
		p.activeCount.Add(1)
		log.Printf("[worker-%d] processing %s (type=%s)", id, task.ID, task.Type)
		result, err := p.dispatch(task)
		p.broker.Acknowledge(task.ID, result, err)
		p.activeCount.Add(-1)
	}
}

func (p *Pool) dispatch(task *models.Task) (string, error) {
	handler, ok := p.handlers[task.Type]
	if !ok {
		return "", fmt.Errorf("no handler for task type: %s", task.Type)
	}
	return handler(task)
}

func (p *Pool) registerDefaultHandlers() {
	p.handlers[models.TaskTypeEmail] = func(task *models.Task) (string, error) {
		time.Sleep(time.Duration(100+rand.Intn(200)) * time.Millisecond)
		if task.Payload["to"] == "" { return "", fmt.Errorf("missing recipient") }
		return fmt.Sprintf("email sent to %s", task.Payload["to"]), nil
	}
	p.handlers[models.TaskTypeResize] = func(task *models.Task) (string, error) {
		time.Sleep(time.Duration(200+rand.Intn(400)) * time.Millisecond)
		return fmt.Sprintf("resized %s to %s", task.Payload["source"], task.Payload["dimensions"]), nil
	}
	p.handlers[models.TaskTypeReport] = func(task *models.Task) (string, error) {
		time.Sleep(time.Duration(400+rand.Intn(500)) * time.Millisecond)
		return fmt.Sprintf("report %s generated", task.Payload["report_id"]), nil
	}
	p.handlers[models.TaskTypeNotify] = func(task *models.Task) (string, error) {
		time.Sleep(time.Duration(50+rand.Intn(100)) * time.Millisecond)
		return fmt.Sprintf("notification sent to %s", task.Payload["user_id"]), nil
	}
}
