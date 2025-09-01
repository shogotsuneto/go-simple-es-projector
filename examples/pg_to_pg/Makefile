.PHONY: help up down clean run producer status logs build

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-15s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

up: ## Start PostgreSQL databases
	docker compose up -d
	@echo "Waiting for databases to be ready..."
	@sleep 5
	@echo "Databases are ready!"

down: ## Stop and remove containers
	docker compose down

clean: ## Stop containers and remove volumes
	docker compose down -v

run: build ## Run the projector (runs continuously until stopped with Ctrl+C)
	@echo "Starting projector... (Press Ctrl+C to stop)"
	./projector

run-once: build ## Run the projector for 10 seconds then stop
	@echo "Running projector for 10 seconds..."
	PROJECTOR_TIMEOUT=10 ./projector
	@echo "Projector stopped after 10 seconds"

producer: build ## Run the event producer
	./producer

build: ## Build the binaries
	cd cmd/projector && go build -o ../../projector main.go
	cd cmd/producer && go build -o ../../producer main.go

status: ## Show container status
	docker compose ps

logs: ## Show database logs
	docker compose logs -f

# Development targets  
dev-reset: clean up ## Reset everything and start fresh
	@echo "Waiting for databases to be ready..."
	@sleep 5
	@echo "Environment reset complete!"

connect-eventstore: ## Connect to event store database
	docker compose exec eventstore-db psql -U eventstore_user -d eventstore

connect-projections: ## Connect to projections database  
	docker compose exec projection-db psql -U projection_user -d projections