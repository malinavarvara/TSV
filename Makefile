postgres:
	docker run --name tsv-processing-app -p 5432:5432 -e POSTGRES_USER=root -e POSTGRES_PASSWORD=secret -d postgres:12-alpine

createdb:
	docker exec -it tsv-processing-app createdb --username=root --owner=root tsv_db

dropdb:
	docker exec -it tsv-processing-app dropdb tsv_db

migrateup:
	migrate -path db/migration -database "postgresql://root:secret@localhost:5432/tsv_db?sslmode=disable" -verbose up

migratedown:
	migrate -path db/migration -database "postgresql://root:secret@localhost:5432/tsv_db?sslmode=disable" -verbose down

sqlc:
	sqlc generate

.PHONY: postgres createdb dropdb migrateup migratedown sqlc