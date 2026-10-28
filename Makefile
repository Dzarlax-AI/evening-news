# Local development commands. Runtime configuration is supplied separately.
COMPOSE = docker compose -f docker-compose.example.yml -f docker-compose.dev.yml
GO_MODULES = go go-web go-background go-worker go-scheduler go-ingestion go-processing go-execution go-editorial go-domain go-tools

.PHONY: help dev stop logs build migrate test vet

help: ## Show available commands
	@awk 'BEGIN {FS = ":.*##"} /^[a-zA-Z_-]+:.*##/ {printf "  %-12s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

dev: ## Start the local development stack
	$(COMPOSE) up -d --build

stop: ## Stop the local development stack without deleting volumes
	$(COMPOSE) down

logs: ## Follow local service logs
	$(COMPOSE) logs -f web background executor

build: ## Build the three core candidate images locally
	REVISION="$$(git rev-parse HEAD)" docker buildx bake -f docker-bake.hcl --load

migrate: ## Run the explicit one-shot migration against the configured local database
	$(COMPOSE) run --rm migrate

test: ## Run Go tests (database integration tests require an isolated test DSN)
	@set -e; for module in $(GO_MODULES); do echo "Testing $$module"; (cd "$$module" && go test ./...); done

vet: ## Run Go static checks
	@set -e; for module in $(GO_MODULES); do echo "Checking $$module"; (cd "$$module" && go vet ./...); done
