BINARY=taskqueue
CMD=./cmd/server

.PHONY: build run test tidy clean docker-up docker-down docker-logs

build:
	go build -o bin/$(BINARY) $(CMD)

run:
	go run $(CMD)/main.go

test:
	go test ./... -v -race -count=1

tidy:
	go mod tidy

clean:
	rm -rf bin/

docker-up:
	docker compose up --build -d

docker-down:
	docker compose down

docker-logs:
	docker compose logs -f server
