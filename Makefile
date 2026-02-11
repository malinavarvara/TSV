# === Variables ===
COMPOSE ?= docker compose
PROJECT_NAME ?= tsvprocessingservice

DB_CONTAINER ?= tsv-processing-db
DB_USER ?= root
DB_PASSWORD ?= secret
DB_NAME ?= tsv_db
DB_PORT ?= 5432

MIGRATION_PATH ?= db/migration
MIGRATE_IMAGE ?= migrate/migrate:v4.15.2
MIGRATE_DB_URL ?= postgresql://$(DB_USER):$(DB_PASSWORD)@localhost:$(DB_PORT)/$(DB_NAME)?sslmode=disable

# === Docker Compose commands ===
build:
	$(COMPOSE) build

up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f app

restart:
	$(COMPOSE) restart

# === Database console ===
psql:
	docker exec -it $(DB_CONTAINER) psql -U $(DB_USER) -d $(DB_NAME)

# === Migrations (run on host, connects to DB container) ===
migrateup:
	docker run --rm --network $(PROJECT_NAME)_default \
		-v $(PWD)/$(MIGRATION_PATH):/migrations \
		$(MIGRATE_IMAGE) \
		-path=/migrations \
		-database "$(MIGRATE_DB_URL)" up

migratedown:
	docker run --rm --network $(PROJECT_NAME)_default \
		-v $(PWD)/$(MIGRATION_PATH):/migrations \
		$(MIGRATE_IMAGE) \
		-path=/migrations \
		-database "$(MIGRATE_DB_URL)" down

# === Full development cycle ===
dev:
	$(COMPOSE) up --build

# === Clean everything (including volumes) ===
clean:
	$(COMPOSE) down -v
	docker system prune -f

# === SQLC code generation (local, requires sqlc installed) ===
sqlc:
	sqlc generate

.PHONY: build up down logs restart psql migrateup migratedown dev clean sqlc