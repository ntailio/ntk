VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X github.com/ntailio/ntk/buildinfo.Version=$(VERSION)

.PHONY: build install test test-short vet lint fmt check sandbox-up sandbox-down sandbox-reset demos

build: ## Build bin/ntk
	go build -trimpath -ldflags '$(LDFLAGS)' -o bin/ntk ./cmd/ntk

install: ## Install ntk into $GOBIN
	go install -trimpath -ldflags '$(LDFLAGS)' ./cmd/ntk

test: ## Unit + integration tests (integration tests skip if the sandbox is down)
	go test ./...

test-short: ## Unit tests only
	go test -short ./...

vet: ## go vet
	go vet ./...

fmt: ## Format all Go files
	gofmt -w .

lint: ## gofmt, go mod tidy, go vet (linux, darwin, windows), staticcheck
	@test -z "$$(gofmt -l .)" || { echo "gofmt needed:"; gofmt -l .; exit 1; }
	go mod tidy -diff
	go vet ./...
	GOOS=darwin go vet ./...
	GOOS=windows go vet ./...
	staticcheck ./...

check: lint ## lint, then all tests
	go test ./...

sandbox-up: ## Start the sandbox cluster (spec/testing.md)
	docker compose up -d --wait

sandbox-down: ## Stop the sandbox, keep data
	docker compose down

sandbox-reset: ## Stop the sandbox and wipe all data
	docker compose down -v

demos: build ## Render assets/demos/*.gif (needs vhs, ttyd, tmux, and the sandbox)
	for t in assets/demos/*.tape; do [ "$$t" = assets/demos/config.tape ] || vhs "$$t" || exit 1; done

help: ## List targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-14s %s\n", $$1, $$2}'
