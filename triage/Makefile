VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)

.PHONY: build test lint capture

build: ## static linux/amd64 binary for the Slurm controller
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags "$(LDFLAGS)" -o bin/node-triage ./cmd/node-triage

test:
	go test ./...

lint:
	test -z "$$(gofmt -l .)" && go vet ./...

capture: ## re-capture scenario fixtures from the lab (breaks and restores the cluster)
	./scripts/capture-scenarios.sh
