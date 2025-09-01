.PHONY: help test build fmt vet lint clean

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-15s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

test: ## Run tests with cache disabled
	go test -count=1 ./...

build: ## Build all packages
	go build ./...

fmt: ## Format code
	go fmt ./...

vet: ## Run static analysis
	go vet ./...

lint: fmt vet ## Run linting (format + vet)

clean: ## Clean build cache
	go clean -cache -modcache

all: fmt vet build test ## Run all checks and tests