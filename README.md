# go-task-queue

![CI](https://github.com/Nitin172503/go-task-queue/actions/workflows/ci.yml/badge.svg)

A priority-based distributed task queue in Go with a pluggable broker interface, concurrent worker pool, and REST API.

## Architecture
─────────────────────────────────────────────────┐
│                  REST API (gorilla/mux)          │
│   POST /tasks   GET /tasks   GET /tasks/:id      │
│   GET /stats    GET /healthz                     │
└────────────────────┬────────────────────────────┘
│
▼
┌─────────────────────────────────────────────────┐
│              Broker Interface                    │
│   Enqueue │ Dequeue │ Acknowledge │ ListTasks    │
└──────┬──────────────────────┬───────────────────┘
│                      │
MemoryBroker           RedisBroker
container/heap         Sorted Set (ZSet)
sync.Mutex             JSON + TTL storage
│                      │
└──────────┬───────────┘
▼
┌─────────────────────────────────────────────────┐
│          Worker Pool (5 goroutines)              │
│   Poll → Dispatch → Handler → Acknowledge        │
│   Retry with priority decay, graceful shutdown   │
└────────────────────────────────
## Brokers

Switch brokers via env var — zero application code changes:
```bash
# In-memory (default)
make run

# Redis
BROKER=redis make run
```

## API

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | /tasks | Enqueue a task |
| GET | /tasks | List tasks (optional ?status= filter) |
| GET | /tasks/:id | Get task by ID |
| GET | /stats | Queue metrics |
| GET | /healthz | Health check |

## Quick Start
```bash
git clone https://github.com/Nitin172503/go-task-queue
cd go-task-queue
go mod tidy
make run
```

## Run Tests
```bash
make test
```

## Task Types

| Type | Payload Keys |
|------|-------------|
| email | to, subject |
| image_resize | source, dimensions |
| generate_report | report_id |
| notification | user_id, message |
