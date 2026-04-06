BINARY=taskqueue
CMD=./cmd/server

.PHONY: build run test tidy clean

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
