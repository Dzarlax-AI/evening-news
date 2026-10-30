# Local development commands. Runtime configuration is supplied separately.
COMPOSE = docker compose --project-directory . -f deploy/compose/development.yml -f deploy/compose/development.override.yml
MANAGED_COMPOSE = docker compose -f docker-compose.managed.yml
GO_MODULES = go go-web go-background go-worker go-scheduler go-ingestion go-processing go-execution go-editorial go-domain go-tools

.PHONY: help dev stop logs build migrate test vet managed-prepare managed-up managed-token managed-logs managed-stop frontend-build

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

frontend-build: ## Install the pinned compiler and build browser assets
	npm --prefix web ci --ignore-scripts --no-audit --no-fund
	npm --prefix web run build

test: frontend-build ## Run Go tests (database integration tests require an isolated test DSN)
	@set -e; for module in $(GO_MODULES); do echo "Testing $$module"; (cd "$$module" && go test ./...); done

vet: ## Run Go static checks
	@set -e; for module in $(GO_MODULES); do echo "Checking $$module"; (cd "$$module" && go vet ./...); done

managed-prepare: ## Create or verify private state for a fresh managed local install
	$(MANAGED_COMPOSE) --profile setup run --rm setup prepare

managed-up: managed-prepare ## Build and start the managed local profile
	$(MANAGED_COMPOSE) up --build -d

managed-token: ## Issue a one-time token to claim the first admin account
	$(MANAGED_COMPOSE) --profile setup run --rm setup setup-token

managed-logs: ## Follow managed profile logs
	$(MANAGED_COMPOSE) logs -f web background executor extractor postgres

managed-stop: ## Stop the managed profile and preserve its installation volumes
	$(MANAGED_COMPOSE) down
