# Autonomous Data Pipeline Orchestrator Makefile
# Provides convenient commands for development, testing, and deployment

.PHONY: help setup up down logs test fmt lint clean install-deps validate

# Default target
.DEFAULT_GOAL := help

# Variables
DOCKER_COMPOSE_FILES := -f docker-compose.yml
KAFKA_COMPOSE_FILES := -f kafka/docker-compose.kafka.yml
PYTHON_DIRS := agent airflow/dags ops tests spark/jobs kafka/producer
AIRFLOW_UID := $(shell id -u)

help: ## Show this help message
	@echo "Autonomous Data Pipeline Orchestrator"
	@echo "====================================="
	@echo ""
	@echo "Available commands:"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

setup: ## Initial setup - copy env file and set permissions
	@echo "Setting up the data pipeline environment..."
	@if [ ! -f .env ]; then \
		cp .env.example .env; \
		echo "Created .env file from .env.example"; \
		echo "Please edit .env file with your configuration"; \
	fi
	@mkdir -p data/ops data/out/parquet data/checkpoints data/quarantine
	@mkdir -p airflow/logs airflow/config
	@echo "AIRFLOW_UID=$(AIRFLOW_UID)" >> .env
	@echo "Setup complete! Edit .env file as needed."

install-deps: ## Install Python dependencies for development
	@echo "Installing Python dependencies..."
	@pip install -r agent/requirements.txt
	@pip install -r kafka/producer/requirements.txt
	@pip install -r airflow/requirements.txt
	@pip install pytest black isort ruff
	@echo "Dependencies installed."

up: ## Start all services (Kafka + Airflow + Spark + Producer)
	@echo "Starting the autonomous data pipeline..."
	@export AIRFLOW_UID=$(AIRFLOW_UID) && docker-compose $(DOCKER_COMPOSE_FILES) up -d
	@echo "Services starting..."
	@echo "Airflow UI: http://localhost:8080 (airflow/airflow)"
	@echo "Kafka UI: http://localhost:8080 (for Kafka monitoring)"
	@echo "Spark Master UI: http://localhost:8082"
	@echo "MailHog UI: http://localhost:8025"
	@echo ""
	@echo "Wait for services to be ready, then trigger the pipeline DAG in Airflow."

up-kafka: ## Start only Kafka services for development
	@echo "Starting Kafka services..."
	@docker-compose $(KAFKA_COMPOSE_FILES) up -d
	@echo "Kafka services started."
	@echo "Kafka UI: http://localhost:8080"

down: ## Stop all services and remove containers
	@echo "Stopping all services..."
	@docker-compose $(DOCKER_COMPOSE_FILES) down
	@docker-compose $(KAFKA_COMPOSE_FILES) down 2>/dev/null || true
	@echo "Services stopped."

down-v: ## Stop all services and remove containers with volumes
	@echo "Stopping all services and removing volumes..."
	@docker-compose $(DOCKER_COMPOSE_FILES) down -v
	@docker-compose $(KAFKA_COMPOSE_FILES) down -v 2>/dev/null || true
	@echo "Services and volumes removed."

logs: ## Show logs from all services
	@docker-compose $(DOCKER_COMPOSE_FILES) logs -f

logs-airflow: ## Show only Airflow logs
	@docker-compose $(DOCKER_COMPOSE_FILES) logs -f airflow-webserver airflow-scheduler

logs-kafka: ## Show only Kafka logs
	@docker-compose $(DOCKER_COMPOSE_FILES) logs -f broker zookeeper

logs-spark: ## Show only Spark logs
	@docker-compose $(DOCKER_COMPOSE_FILES) logs -f spark-master spark-worker

logs-producer: ## Show only producer logs
	@docker-compose $(DOCKER_COMPOSE_FILES) logs -f producer

status: ## Show status of all services
	@echo "Service Status:"
	@echo "==============="
	@docker-compose $(DOCKER_COMPOSE_FILES) ps

test: ## Run all tests
	@echo "Running unit tests..."
	@python -m pytest tests/ -v --tb=short
	@echo "Tests completed."

test-agent: ## Run agent tests only
	@echo "Running agent tests..."
	@python -m pytest tests/test_agent.py -v

test-dq: ## Run data quality tests only
	@echo "Running data quality tests..."
	@python -m pytest tests/test_dq.py -v

test-spark: ## Run Spark job tests only
	@echo "Running Spark job tests..."
	@python -m pytest tests/test_spark_jobs.py -v

test-coverage: ## Run tests with coverage report
	@echo "Running tests with coverage..."
	@python -m pytest tests/ --cov=agent --cov=ops --cov-report=html --cov-report=term
	@echo "Coverage report generated in htmlcov/"

fmt: ## Format code with black and isort
	@echo "Formatting code..."
	@black $(PYTHON_DIRS)
	@isort $(PYTHON_DIRS)
	@echo "Code formatted."

lint: ## Run linting with ruff
	@echo "Running linter..."
	@ruff check $(PYTHON_DIRS)
	@echo "Linting completed."

lint-fix: ## Run linting with auto-fix
	@echo "Running linter with auto-fix..."
	@ruff check --fix $(PYTHON_DIRS)
	@echo "Linting completed with fixes applied."

validate: ## Validate configuration files
	@echo "Validating configuration files..."
	@python -c "import json; json.load(open('kafka/schemas/customer_events.avsc'))" && echo "✓ Kafka schema is valid JSON"
	@python -c "import json; json.load(open('dq/expectations/customers_expectation_suite.json'))" && echo "✓ GE expectations are valid JSON"
	@python -c "import yaml; yaml.safe_load(open('airflow/include/config.yml'))" && echo "✓ Airflow config is valid YAML" || echo "! YAML validation requires PyYAML"
	@echo "Configuration validation completed."

clean: ## Clean up temporary files and caches
	@echo "Cleaning up..."
	@find . -type f -name "*.pyc" -delete
	@find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	@find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	@find . -type d -name "htmlcov" -exec rm -rf {} + 2>/dev/null || true
	@find . -type f -name ".coverage" -delete 2>/dev/null || true
	@echo "Cleanup completed."

reset-data: ## Reset all data volumes (WARNING: destroys all data)
	@echo "WARNING: This will delete ALL data in the pipeline!"
	@read -p "Are you sure? Type 'yes' to continue: " confirm && [ "$$confirm" = "yes" ] || exit 1
	@make down-v
	@docker volume prune -f
	@rm -rf data/ops/* data/out/parquet/* data/checkpoints/* data/quarantine/* 2>/dev/null || true
	@echo "All data reset."

producer-test: ## Run producer in test mode (short duration)
	@echo "Running producer in test mode..."
	@docker-compose $(DOCKER_COMPOSE_FILES) exec producer python3 producer/produce_events.py --rate 10 --duration 60 --late-rate 0.1 --drift-frequency 20

spark-submit: ## Submit Spark job manually for testing
	@echo "Submitting Spark job..."
	@docker-compose $(DOCKER_COMPOSE_FILES) exec spark-master spark-submit \
		--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.apache.spark:spark-avro_2.12:3.4.0 \
		/opt/spark/work-dir/jobs/ingest_transform.py

airflow-trigger: ## Trigger the main pipeline DAG in Airflow
	@echo "Triggering pipeline DAG..."
	@curl -X POST "http://localhost:8080/api/v1/dags/autonomous_data_pipeline/dagRuns" \
		-H "Content-Type: application/json" \
		-u "airflow:airflow" \
		-d '{"dag_run_id": "manual_trigger_'$(shell date +%Y%m%d_%H%M%S)'"}' || \
		echo "Failed to trigger DAG. Make sure Airflow is running and accessible."

monitor: ## Show real-time pipeline monitoring
	@echo "Pipeline Monitoring Dashboard"
	@echo "============================="
	@echo "Press Ctrl+C to stop monitoring"
	@while true; do \
		clear; \
		echo "Pipeline Status - $(shell date)"; \
		echo "================================"; \
		echo ""; \
		echo "Docker Services:"; \
		docker-compose $(DOCKER_COMPOSE_FILES) ps --format "table {{.Service}}\t{{.Status}}\t{{.Ports}}" 2>/dev/null || echo "Services not running"; \
		echo ""; \
		echo "Recent Data:"; \
		find data/ops -name "*.json" -type f -exec basename {} \; 2>/dev/null | head -5 || echo "No data files found"; \
		echo ""; \
		echo "Data Volume Usage:"; \
		du -sh data/ 2>/dev/null || echo "No data directory"; \
		sleep 5; \
	done

debug: ## Show debug information
	@echo "Debug Information"
	@echo "=================="
	@echo "Environment:"
	@echo "  Docker: $(shell docker --version 2>/dev/null || echo 'Not installed')"
	@echo "  Docker Compose: $(shell docker-compose --version 2>/dev/null || echo 'Not installed')"
	@echo "  Python: $(shell python3 --version 2>/dev/null || echo 'Not installed')"
	@echo "  User ID: $(AIRFLOW_UID)"
	@echo ""
	@echo "Configuration:"
	@echo "  .env file exists: $(shell test -f .env && echo 'Yes' || echo 'No')"
	@echo "  Data directory: $(shell test -d data && echo 'Exists' || echo 'Missing')"
	@echo ""
	@echo "Docker Status:"
	@docker system df 2>/dev/null || echo "Docker not running"

docs: ## Generate documentation (if available)
	@echo "Documentation links:"
	@echo "==================="
	@echo "Airflow UI: http://localhost:8080"
	@echo "Kafka UI: http://localhost:8080"
	@echo "Spark Master UI: http://localhost:8082"
	@echo "MailHog UI: http://localhost:8025"
	@echo ""
	@echo "API Endpoints:"
	@echo "Airflow API: http://localhost:8080/api/v1/"
	@echo "Schema Registry: http://localhost:8081/"

# Development workflow targets
dev-setup: setup install-deps ## Complete development setup
	@echo "Development environment setup complete!"
	@echo "Next steps:"
	@echo "1. Edit .env file with your configuration"
	@echo "2. Run 'make up' to start all services"
	@echo "3. Open Airflow UI at http://localhost:8080"

ci-test: fmt lint test ## Run CI pipeline locally
	@echo "CI pipeline completed successfully!"

# Quick start for new users
quick-start: dev-setup up ## Quick start for new users
	@echo "Quick start completed!"
	@echo "Services are starting up..."
	@echo "Wait 2-3 minutes for all services to be ready."