VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X github.com/ntailio/ntk/buildinfo.Version=$(VERSION)

.PHONY: build install test test-short vet lint fmt check dist docker release-branch sandbox-up sandbox-down sandbox-reset demos

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
	go run github.com/rhysd/actionlint/cmd/actionlint@v1.7.12

check: lint ## lint, then all tests
	go test ./...

DIST_TARGETS := linux/amd64 linux/arm64 linux/arm darwin/amd64 darwin/arm64 windows/amd64 windows/arm64 freebsd/amd64

dist: ## Release binaries and checksums in dist/ (VERSION=v1.2.3)
	rm -rf dist && mkdir dist
	@for t in $(DIST_TARGETS); do \
		os=$${t%/*}; arch=$${t#*/}; name=$$arch; ext=; \
		[ $$arch = arm ] && name=armv7; [ $$os = windows ] && ext=.exe; \
		echo "  dist/ntk-$(VERSION)-$$os-$$name$$ext"; \
		CGO_ENABLED=0 GOOS=$$os GOARCH=$$arch GOARM=7 go build -trimpath -ldflags '$(LDFLAGS)' \
			-o dist/ntk-$(VERSION)-$$os-$$name$$ext ./cmd/ntk || exit 1; \
	done
	cd dist && sha256sum ntk-* > checksums.tmp && mv checksums.tmp ntk-$(VERSION)-checksums.txt

docker: ## Build the Docker image as ntk:dev
	docker build --build-arg VERSION=$(VERSION) -t ntk:dev .

release-branch: ## Cut release/$(VERSION) from trunk with a changelog template (VERSION=v1.2.3)
	@test "$(origin VERSION)" = "command line" || { echo "usage: make release-branch VERSION=v1.2.3"; exit 2; }
	scripts/release-branch.sh $(VERSION)

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
