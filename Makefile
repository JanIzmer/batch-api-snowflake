.DEFAULT_GOAL := help
SHELL := /bin/bash

DATE ?= $(shell date -u -d '2 days ago' +%F 2>/dev/null || date -u -v-2d +%F)
START ?= $(DATE)
END ?= $(DATE)

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

.PHONY: install
install: ## Create a local venv with dev dependencies
	python3 -m venv .venv
	./.venv/bin/pip install --upgrade pip
	./.venv/bin/pip install -r requirements-dev.txt
	./.venv/bin/pip install -e .

.PHONY: lint
lint: ## Ruff + mypy
	./.venv/bin/ruff check src tests
	./.venv/bin/ruff format --check src tests
	./.venv/bin/mypy

.PHONY: fmt
fmt: ## Autoformat
	./.venv/bin/ruff format src tests
	./.venv/bin/ruff check --fix src tests

.PHONY: test
test: ## Unit tests
	./.venv/bin/pytest --cov=pipeline --cov-report=term-missing

.PHONY: up
up: ## Start the local Airflow stack
	docker compose up -d --build
	@echo "Airflow UI: http://localhost:8080 (admin / admin)"

.PHONY: down
down: ## Stop the stack, keep volumes
	docker compose down

.PHONY: clean
clean: ## Stop the stack and delete volumes and local data
	docker compose down -v
	rm -rf data/landing data/quarantine dbt/target dbt/dbt_packages

.PHONY: apply-ddl
apply-ddl: ## Create Snowflake objects (idempotent)
	docker compose run --rm cli weather-pipeline apply-ddl

.PHONY: ingest
ingest: ## Ingest one day: make ingest DATE=2026-08-20
	docker compose run --rm cli weather-pipeline ingest --date $(DATE)

.PHONY: backfill
backfill: ## Backfill a range: make backfill START=2026-08-01 END=2026-08-31
	docker compose run --rm cli weather-pipeline ingest --start $(START) --end $(END)

.PHONY: dbt-build
dbt-build: ## dbt deps + seed + build
	docker compose run --rm cli bash -lc "cd /opt/pipeline/dbt && dbt deps && dbt seed && dbt build"

.PHONY: dbt-test
dbt-test: ## dbt test with stored failures
	docker compose run --rm cli bash -lc "cd /opt/pipeline/dbt && dbt test --store-failures"

.PHONY: dbt-docs
dbt-docs: ## Generate and serve the dbt docs site
	docker compose run --rm --service-ports cli bash -lc \
		"cd /opt/pipeline/dbt && dbt docs generate && dbt docs serve --port 8081 --host 0.0.0.0"

.PHONY: sqlfluff
sqlfluff: ## Lint the dbt SQL
	./.venv/bin/sqlfluff lint dbt/models dbt/tests
