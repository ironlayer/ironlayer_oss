.PHONY: install lint format test test-unit test-integration test-e2e test-benchmark test-slow migrate docker-up docker-down clean backup restore test-backup-restore sync-rules rollback

INFRA_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
OSS_DIR   := $(INFRA_DIR)../ironlayer_OSS

install:
	uv sync --all-packages
	cd frontend && npm install

lint:
	uv run ruff check core_engine/ ai_engine/ api/ cli/
	uv run --package ironlayer-core mypy core_engine/
	uv run --package ai-engine mypy ai_engine/
	uv run --package ironlayer-api mypy api/
	uv run --package ironlayer mypy cli/

format:
	uv run ruff format core_engine/ ai_engine/ api/ cli/
	uv run ruff check --fix core_engine/ ai_engine/ api/ cli/

test: test-unit test-integration

test-unit:
	uv run --package ironlayer-core pytest core_engine/tests/unit/ -v --cov=core_engine --cov-report=term-missing --cov-fail-under=70
	uv run --package ai-engine pytest ai_engine/tests/ -v --cov=ai_engine --cov-report=term-missing --cov-fail-under=75
	uv run --package ironlayer-api pytest api/tests/ -v --cov=api --cov-report=term-missing --cov-fail-under=75
	uv run --package ironlayer pytest cli/tests/ -v --cov=cli --cov-report=term-missing

test-integration:
	uv run --package ironlayer-core pytest core_engine/tests/integration/ -v

test-e2e:
	uv run --package ironlayer-core pytest core_engine/tests/e2e/ -v

migrate:
	uv run --package ironlayer-core alembic -c core_engine/state/migrations/alembic.ini upgrade head

migrate-create:
	uv run --package ironlayer-core alembic -c core_engine/state/migrations/alembic.ini revision --autogenerate -m "$(msg)"

docker-up:
	docker compose up -d

docker-down:
	docker compose down

docker-build:
	docker compose build

backup:
	bash infra/scripts/backup.sh

restore:
	bash infra/scripts/restore.sh $(BACKUP_FILE)

test-backup-restore:
	bash infra/scripts/test_backup_restore.sh

test-benchmark:
	uv run --package ironlayer-core pytest core_engine/tests/benchmark/ -v -m benchmark

test-slow:
	uv run --package ai-engine pytest ai_engine/tests/ -v -m slow

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .mypy_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .ruff_cache -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true

# Emergency rollback: restore the previous revision to 100% traffic.
# Requires RESOURCE_GROUP and optionally APP_PREFIX env vars.
# Usage: make rollback                          (rolls back all three services)
#        make rollback TARGET=api               (rolls back API only)
#        RESOURCE_GROUP=ironlayer-prod make rollback
rollback:
	APP_PREFIX=$${APP_PREFIX:-ironlayer} \
	RESOURCE_GROUP=$${RESOURCE_GROUP:?Set RESOURCE_GROUP to the Azure resource group name} \
	  bash $(INFRA_DIR)infra/scripts/rollback.sh $${TARGET:-all}

# Sync non-sensitive AI config files from infra (canonical) → OSS repo.
# Run after updating CLAUDE.md or AGENTS.md before committing both repos.
# BACKLOG.md and LESSONS.md are private and are intentionally NOT synced.
sync-rules:
	cp $(INFRA_DIR)/CLAUDE.md $(OSS_DIR)/CLAUDE.md
	cp $(INFRA_DIR)/AGENTS.md $(OSS_DIR)/AGENTS.md
	@echo "Rules synced: infra → OSS"
	@echo "Review 'git diff' in ironlayer_OSS before committing."
