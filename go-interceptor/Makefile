# go-interceptor. `make help` lists everything.
#
# GOFLAGS=-mod=vendor everywhere: dependencies are vendored so no target ever
# reaches out to a module proxy. See README.md for why that matters here.

BIN      := bin/vp-fms-interceptor
CONFIG   := ../config.toml
GOFLAGS  := -mod=vendor
IMAGE    := vp-fms-interceptor:go
export GOFLAGS

.PHONY: help build test race vet lint run image clean deps

help:
	@grep -E '^[a-z-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  %-8s %s\n", $$1, $$2}'

build: ## Build the binary into bin/
	go build -trimpath -ldflags="-s -w" -o $(BIN) ./cmd/interceptor

test: ## Run the unit and integration tests
	go test ./...

race: ## Run the tests under the race detector (the confinement check)
	go test -race -count=1 ./...

vet: ## go vet
	go vet ./...

check: vet race ## Everything CI should run

run: build ## Build and run against ../config.toml
	./$(BIN) $(CONFIG)

image: ## Build the docker image (context is the PROJECT ROOT)
	cd .. && docker build -f go-interceptor/Dockerfile -t $(IMAGE) .

deps: ## Re-resolve and re-vendor dependencies (needs GOPROXY=direct on this network)
	GOFLAGS=-mod=mod GOPROXY=direct go mod tidy
	GOFLAGS=-mod=mod go mod vendor

clean: ## Remove build output
	rm -rf bin/
