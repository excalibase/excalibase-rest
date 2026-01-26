# Excalibase REST API Makefile
# Provides e2e testing and development commands

# Configuration
COMPOSE_PROJECT = excalibase-rest-app
COMPOSE_FILE = docker-compose.yml
APP_PORT = 20000
DB_PORT = 5432
API_URL = http://localhost:$(APP_PORT)/api/v1

# Colors for output
BLUE = \033[0;34m
GREEN = \033[0;32m
YELLOW = \033[1;33m
RED = \033[0;31m
NC = \033[0m # No Color

# Default target
.DEFAULT_GOAL := help

# Help target
.PHONY: help
help: ## Show this help message
	@echo "$(BLUE)Excalibase REST API - Available Commands$(NC)"
	@echo ""
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  $(GREEN)%-15s$(NC) %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@echo ""
	@echo "$(YELLOW)Examples:$(NC)"
	@echo "  make e2e            # Complete e2e test (build + test + cleanup)"
	@echo "  make dev            # Build then start services and keep running"
	@echo "  make up             # Pull and start service from docker hub "
	@echo "  make test-only      # Run tests against running services"
	@echo "  make clean          # Stop services and cleanup"
	@echo ""
	@echo "$(YELLOW)Development:$(NC)"
	@echo "  make dev-setup      # Setup development database"
	@echo "  make quick-start    # Database + Application startup"
	@echo "  make run            # Run application locally"
	@echo "  make test           # Run all tests"
	@echo ""
	@echo "$(YELLOW)CI/CD Integration:$(NC)"
	@echo "  make ci             # Run complete CI pipeline"

# Main targets
.PHONY: e2e
e2e: down build-image up test clean ## Complete e2e test suite (cleanup, build image, test, cleanup)
	@echo "$(GREEN)🎉 E2E testing completed successfully!$(NC)"

.PHONY: dev
dev: check-deps build-image up ## Start services for development (no cleanup)
	@echo ""
	@echo "$(GREEN)🚀 Development environment ready!$(NC)"
	@echo ""
	@echo "$(BLUE)REST API:$(NC) $(API_URL)"
	@echo "$(BLUE)PostgreSQL:$(NC)  localhost:$(DB_PORT)"
	@echo "$(BLUE)API Docs:$(NC)    $(API_URL)/docs"
	@echo "$(BLUE)OpenAPI:$(NC)     $(API_URL)/openapi.json"
	@echo ""
	@echo "$(YELLOW)To run tests:$(NC) make test-only"
	@echo "$(YELLOW)To cleanup:$(NC)  make clean"
	@echo ""

.PHONY: ci
ci: check-deps-ci build-image up test clean ## CI/CD pipeline (with dependency checks)

# Build targets
.PHONY: build
build: ## Build the application with Maven
	@echo "$(BLUE)🔨 Building application...$(NC)"
	@mvn clean package -DskipTests -q
	@echo "$(GREEN)✓ Build completed$(NC)"

.PHONY: build-image
build-image: build ## Build Docker image locally for e2e testing
	@echo "$(BLUE)🐳 Building Docker image...$(NC)"
	@docker build -t excalibase/excalibase-rest .
	@echo "$(GREEN)✓ Docker image built$(NC)"

.PHONY: build-skip
build-skip: ## Skip Maven build (for rapid iteration)
	@echo "$(YELLOW)⚠️  Skipping Maven build$(NC)"

# Service management
.PHONY: up
up: ## Start Docker services
	@echo "$(BLUE)🚀 Starting services...$(NC)"
	@docker-compose -f $(COMPOSE_FILE) -p $(COMPOSE_PROJECT) down -v --remove-orphans > /dev/null 2>&1 || true
	@docker-compose -f $(COMPOSE_FILE) -p $(COMPOSE_PROJECT) up -d
	@echo "$(GREEN)✓ Services started$(NC)"
	@$(MAKE) --no-print-directory wait-ready

.PHONY: down
down: ## Stop Docker services
	@echo "$(BLUE)🛑 Stopping services...$(NC)"
	@docker-compose -f $(COMPOSE_FILE) -p $(COMPOSE_PROJECT) down -v --remove-orphans > /dev/null 2>&1 || true
	@echo "$(GREEN)✓ Services stopped$(NC)"

.PHONY: clean
clean: down ## Clean up all resources
	@echo "$(BLUE)🧹 Cleaning up...$(NC)"
	@docker system prune -f > /dev/null 2>&1 || true
	@echo "$(GREEN)✓ Cleanup completed$(NC)"

# Development targets
.PHONY: dev-setup
dev-setup: ## Setup development environment with PostgreSQL
	@echo "$(BLUE)🔧 Setting up development environment...$(NC)"
	@docker-compose -f $(COMPOSE_FILE) -p $(COMPOSE_PROJECT) up -d postgres
	@echo "$(GREEN)✓ Development database ready$(NC)"
	@echo "$(YELLOW)Database URL:$(NC) jdbc:postgresql://localhost:$(DB_PORT)/excalibase_rest"
	@echo "$(YELLOW)Username:$(NC) testuser"
	@echo "$(YELLOW)Password:$(NC) testpass"

.PHONY: dev-teardown
dev-teardown: ## Teardown development environment
	@echo "$(BLUE)🔧 Tearing down development environment...$(NC)"
	@docker-compose -f $(COMPOSE_FILE) -p $(COMPOSE_PROJECT) down -v --remove-orphans > /dev/null 2>&1 || true
	@echo "$(GREEN)✓ Development environment cleaned up$(NC)"

.PHONY: quick-start
quick-start: dev-setup run ## Quick start: setup database and run application
	@echo "$(GREEN)🚀 Quick start completed!$(NC)"

.PHONY: run
run: ## Run application locally (requires database)
	@echo "$(BLUE)▶️  Starting application locally...$(NC)"
	@cd modules/excalibase-rest-api && mvn spring-boot:run -Dspring-boot.run.profiles=dev

# Testing targets
.PHONY: test
test: up test-only ## Start services and run tests
	@echo "$(GREEN)✓ Tests completed$(NC)"

.PHONY: test-maven
test-maven: ## Run all Maven tests
	@echo "$(BLUE)🧪 Running all Maven tests...$(NC)"
	@mvn test
	@echo "$(GREEN)✓ All Maven tests passed$(NC)"

.PHONY: test-integration
test-integration: ## Run integration tests only
	@echo "$(BLUE)🧪 Running integration tests...$(NC)"
	@mvn test -Dtest="*IntegrationTest"
	@echo "$(GREEN)✓ Integration tests passed$(NC)"

.PHONY: test-unit
test-unit: ## Run unit tests only
	@echo "$(BLUE)🧪 Running unit tests...$(NC)"
	@mvn test -Dtest="*Test,!*IntegrationTest"
	@echo "$(GREEN)✓ Unit tests passed$(NC)"

.PHONY: test-coverage
test-coverage: ## Run tests with coverage report
	@echo "$(BLUE)📊 Running tests with coverage...$(NC)"
	@mvn clean test jacoco:report
	@echo "$(GREEN)✓ Coverage report generated$(NC)"
	@echo "$(YELLOW)Report location:$(NC) target/site/jacoco/index.html"

.PHONY: test-only
test-only: ## Run E2E tests against running services
	@echo "$(BLUE)🧪 Running E2E tests...$(NC)"
	@$(MAKE) --no-print-directory run-tests

.PHONY: test-quick
test-quick: build-skip up test-only ## Quick test (skip build)
	@echo "$(GREEN)✓ Quick tests completed$(NC)"

# Wait for services
.PHONY: wait-ready
wait-ready: ## Wait for services to be ready
	@echo "$(BLUE)⏳ Waiting for services...$(NC)"
	@for i in $$(seq 1 30); do \
		if docker-compose -f $(COMPOSE_FILE) -p $(COMPOSE_PROJECT) exec -T postgres pg_isready -U testuser -d excalibase_rest > /dev/null 2>&1; then \
			echo "$(GREEN)✓ PostgreSQL ready$(NC)"; \
			break; \
		fi; \
		if [ $$i -eq 30 ]; then \
			echo "$(RED)❌ PostgreSQL failed to start$(NC)"; \
			exit 1; \
		fi; \
		printf "."; \
		sleep 2; \
	done
	@echo "$(BLUE)🔄 Waiting for application...$(NC)"
	@sleep 10
	@echo "$(GREEN)✓ All services ready$(NC)"

.PHONY: run-tests
run-tests: ## Execute the actual test suite
	@./scripts/e2e-test.sh || (echo "$(RED)❌ Tests failed$(NC)" && exit 1)

# Status and logs
.PHONY: status
status: ## Show service status
	@echo "$(BLUE)📊 Service Status$(NC)"
	@docker-compose -f $(COMPOSE_FILE) -p $(COMPOSE_PROJECT) ps

.PHONY: logs
logs: ## Show service logs
	@docker-compose -f $(COMPOSE_FILE) -p $(COMPOSE_PROJECT) logs -f

.PHONY: logs-app
logs-app: ## Show application logs only
	@docker-compose -f $(COMPOSE_FILE) -p $(COMPOSE_PROJECT) logs -f app

.PHONY: logs-db
logs-db: ## Show database logs only
	@docker-compose -f $(COMPOSE_FILE) -p $(COMPOSE_PROJECT) logs -f postgres

# Database utilities
.PHONY: db-shell
db-shell: ## Connect to database shell
	@docker-compose -f $(COMPOSE_FILE) -p $(COMPOSE_PROJECT) exec postgres psql -U testuser -d excalibase_rest

.PHONY: db-reset
db-reset: ## Reset database (recreate schema)
	@echo "$(BLUE)🔄 Resetting database...$(NC)"
	@docker-compose -f $(COMPOSE_FILE) -p $(COMPOSE_PROJECT) exec postgres psql -U testuser -d excalibase_rest -c "DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;"
	@docker-compose -f $(COMPOSE_FILE) -p $(COMPOSE_PROJECT) restart app
	@echo "$(GREEN)✓ Database reset completed$(NC)"

# Docker utilities
.PHONY: docker-build
docker-build: ## Build Docker image
	@echo "$(BLUE)🐳 Building Docker image...$(NC)"
	@docker build -t excalibase/excalibase-rest:latest .
	@echo "$(GREEN)✓ Docker image built$(NC)"

.PHONY: docker-push
docker-push: docker-build ## Push Docker image to registry
	@echo "$(BLUE)📤 Pushing Docker image...$(NC)"
	@docker push excalibase/excalibase-rest:latest
	@echo "$(GREEN)✓ Docker image pushed$(NC)"

.PHONY: docker-run
docker-run: ## Run application in Docker
	@docker run -p $(APP_PORT):20000 \
		-e SPRING_DATASOURCE_URL=jdbc:postgresql://host.docker.internal:$(DB_PORT)/excalibase_rest \
		-e SPRING_DATASOURCE_USERNAME=testuser \
		-e SPRING_DATASOURCE_PASSWORD=testpass \
		excalibase/excalibase-rest:latest

# Dependency checks
.PHONY: check-deps
check-deps: ## Check required dependencies
	@echo "$(BLUE)🔍 Checking dependencies...$(NC)"
	@command -v docker >/dev/null 2>&1 || { echo "$(RED)❌ Docker is required but not installed$(NC)"; exit 1; }
	@command -v docker-compose >/dev/null 2>&1 || { echo "$(RED)❌ Docker Compose is required but not installed$(NC)"; exit 1; }
	@command -v mvn >/dev/null 2>&1 || { echo "$(RED)❌ Maven is required but not installed$(NC)"; exit 1; }
	@command -v java >/dev/null 2>&1 || { echo "$(RED)❌ Java is required but not installed$(NC)"; exit 1; }
	@echo "$(GREEN)✓ All dependencies available$(NC)"

.PHONY: check-deps-ci
check-deps-ci: ## Check dependencies for CI (no Maven required)
	@echo "$(BLUE)🔍 Checking CI dependencies...$(NC)"
	@command -v docker >/dev/null 2>&1 || (echo "$(RED)❌ Docker not found$(NC)" && exit 1)
	@(command -v docker-compose >/dev/null 2>&1 || docker compose version >/dev/null 2>&1) || (echo "$(RED)❌ Docker Compose not found$(NC)" && exit 1)
	@command -v curl >/dev/null 2>&1 || (echo "$(RED)❌ curl not found$(NC)" && exit 1)
	@command -v jq >/dev/null 2>&1 || (echo "$(RED)❌ jq not found$(NC)" && exit 1)
	@echo "$(GREEN)✓ All CI dependencies available$(NC)"

# Package and distribution
.PHONY: package
package: ## Package application for distribution
	@echo "$(BLUE)📦 Packaging application...$(NC)"
	@mvn clean package -DskipTests
	@echo "$(GREEN)✓ Application packaged$(NC)"
	@echo "$(YELLOW)JAR location:$(NC) modules/excalibase-rest-api/target/excalibase-rest-api-*.jar"

.PHONY: install
install: ## Install application to local repository
	@echo "$(BLUE)📦 Installing to local repository...$(NC)"
	@mvn clean install
	@echo "$(GREEN)✓ Application installed$(NC)"

# Documentation
.PHONY: docs-serve
docs-serve: ## Serve documentation locally
	@echo "$(BLUE)📖 Serving documentation...$(NC)"
	@echo "$(YELLOW)Documentation will be available at: http://localhost:8000$(NC)"
	@python3 -m http.server 8000 --directory docs

# Utility targets
.PHONY: install-deps
install-deps: ## Install missing dependencies (macOS)
	@echo "$(BLUE)📦 Installing dependencies...$(NC)"
	@if ! command -v jq >/dev/null 2>&1; then \
		echo "$(YELLOW)Installing jq...$(NC)"; \
		brew install jq; \
	fi
	@if ! command -v docker >/dev/null 2>&1; then \
		echo "$(YELLOW)Please install Docker Desktop from: https://docker.com/products/docker-desktop$(NC)"; \
	fi
	@echo "$(GREEN)✓ Dependencies check completed$(NC)"

# Development workflow targets
.PHONY: restart
restart: down up ## Restart services
	@echo "$(GREEN)✓ Services restarted$(NC)"

.PHONY: rebuild
rebuild: clean build up ## Full rebuild and restart
	@echo "$(GREEN)✓ Full rebuild completed$(NC)"

# Health checks
.PHONY: health
health: ## Check API health
	@echo "$(BLUE)🏥 Checking API health...$(NC)"
	@curl -sf $(API_URL) > /dev/null && echo "$(GREEN)✓ API is healthy$(NC)" || echo "$(RED)❌ API is not responding$(NC)"

.PHONY: test-connection
test-connection: ## Test database connection
	@echo "$(BLUE)🔗 Testing database connection...$(NC)"
	@docker-compose -f $(COMPOSE_FILE) -p $(COMPOSE_PROJECT) exec postgres pg_isready -U testuser && echo "$(GREEN)✓ Database connection OK$(NC)" || echo "$(RED)❌ Database connection failed$(NC)"

# Utility targets
.PHONY: version
version: ## Show application version
	@mvn help:evaluate -Dexpression=project.version -q -DforceStdout

.PHONY: info
info: ## Show project information
	@echo "$(BLUE)📋 Project Information$(NC)"
	@echo "$(YELLOW)Name:$(NC) Excalibase REST API"
	@echo "$(YELLOW)Version:$(NC) $$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)"
	@echo "$(YELLOW)Java Version:$(NC) $$(mvn help:evaluate -Dexpression=maven.compiler.release -q -DforceStdout)"
	@echo "$(YELLOW)Spring Boot Version:$(NC) $$(mvn help:evaluate -Dexpression=spring-boot.version -q -DforceStdout)"
	@echo "$(YELLOW)API URL:$(NC) $(API_URL)"
	@echo "$(YELLOW)Database Port:$(NC) $(DB_PORT)"