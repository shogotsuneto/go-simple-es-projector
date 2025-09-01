.PHONY: help up down clean run producer status logs build

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-15s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

up: ## Start PostgreSQL databases
	docker-compose up -d
	@echo "Waiting for databases to be ready..."
	@sleep 5
	@echo "Databases are ready!"

down: ## Stop and remove containers
	docker-compose down

clean: ## Stop containers and remove volumes
	docker-compose down -v

run: build ## Run the projector
	./projector

producer: build ## Run the event producer
	./producer

build: ## Build the binaries
	go build -o projector main.go
	cd cmd/producer && go build -o ../../producer main.go

status: ## Show container status
	docker-compose ps

logs: ## Show database logs
	docker-compose logs -f

# Complete workflow targets
demo: up wait-db run ## Full demo: start databases, wait, then run projector
	@echo "Demo completed! Try 'make producer' then 'make run' again to see new events."

wait-db: ## Wait for databases to be ready
	@echo "Waiting for databases to be healthy..."
	@timeout 60 bash -c 'until docker-compose exec -T eventstore-db pg_isready -U eventstore_user -d eventstore >/dev/null 2>&1; do sleep 1; done'
	@timeout 60 bash -c 'until docker-compose exec -T projection-db pg_isready -U projection_user -d projections >/dev/null 2>&1; do sleep 1; done'
	@echo "Databases are healthy!"

full-cycle: up wait-db run producer run ## Complete cycle: start, project initial data, add events, project again

# Development targets  
dev-reset: clean up wait-db ## Reset everything and start fresh
	@echo "Environment reset complete!"

connect-eventstore: ## Connect to event store database
	docker-compose exec eventstore-db psql -U eventstore_user -d eventstore

connect-projections: ## Connect to projections database  
	docker-compose exec projection-db psql -U projection_user -d projections