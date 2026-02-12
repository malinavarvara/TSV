package database

import (
	"TSVProcessingService/internal/config"
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestDB(t *testing.T) (*Store, func()) {
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "postgres:15-alpine",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "test",
			"POSTGRES_PASSWORD": "test",
			"POSTGRES_DB":       "testdb",
		},
		WaitingFor: wait.ForSQL("5432/tcp", "postgres", func(host string, port nat.Port) string {
			return fmt.Sprintf("postgres://test:test@%s:%s/testdb?sslmode=disable", host, port.Port())
		}).WithStartupTimeout(60 * time.Second),
		Cmd: []string{"postgres", "-c", "fsync=off"},
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err)

	host, err := container.Host(ctx)
	require.NoError(t, err)
	port, err := container.MappedPort(ctx, "5432")
	require.NoError(t, err)

	dsn := fmt.Sprintf("host=%s port=%d user=test password=test dbname=testdb sslmode=disable",
		host, port.Int())
	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)

	// Проверяем подключение
	err = db.PingContext(ctx)
	require.NoError(t, err)

	// Выполняем миграции
	migrationSQL := `
	CREATE TABLE IF NOT EXISTS files (
		id SERIAL PRIMARY KEY,
		filename VARCHAR(255) NOT NULL UNIQUE,
		file_hash VARCHAR(64),
		status VARCHAR(50),
		rows_processed INTEGER,
		rows_failed INTEGER,
		error_message TEXT,
		created_at TIMESTAMP DEFAULT NOW(),
		updated_at TIMESTAMP DEFAULT NOW()
	);
	CREATE TABLE IF NOT EXISTS device_data (
		id SERIAL PRIMARY KEY,
		file_id INTEGER REFERENCES files(id),
		unit_guid UUID NOT NULL,
		mqtt VARCHAR(50),
		invid VARCHAR(50),
		msg_id VARCHAR(100),
		text TEXT,
		context VARCHAR(50),
		class VARCHAR(50),
		level INTEGER,
		area VARCHAR(50),
		addr VARCHAR(100),
		block VARCHAR(50),
		type VARCHAR(50),
		bit INTEGER,
		invert_bit BOOLEAN,
		line_number INTEGER,
		created_at TIMESTAMP DEFAULT NOW()
	);
	CREATE TABLE IF NOT EXISTS processing_errors (
		id SERIAL PRIMARY KEY,
		file_id INTEGER REFERENCES files(id),
		line_number INTEGER,
		raw_line TEXT,
		error_message TEXT,
		field_name VARCHAR(100),
		created_at TIMESTAMP DEFAULT NOW()
	);
	CREATE TABLE IF NOT EXISTS reports (
		id SERIAL PRIMARY KEY,
		unit_guid UUID NOT NULL,
		report_type VARCHAR(20),
		file_path TEXT NOT NULL,
		generated_at TIMESTAMP DEFAULT NOW()
	);
	CREATE TABLE IF NOT EXISTS api_logs (
		id SERIAL PRIMARY KEY,
		method VARCHAR(10),
		endpoint TEXT,
		status_code INTEGER,
		duration_ms INTEGER,
		created_at TIMESTAMP DEFAULT NOW()
	);
	`
	_, err = db.ExecContext(ctx, migrationSQL)
	require.NoError(t, err)

	store := NewStore(db)

	cleanup := func() {
		db.Close()
		container.Terminate(ctx)
	}
	return store, cleanup
}

func TestStore_CountDeviceDataByUnit(t *testing.T) {
	store, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()

	// Вставляем тестовые данные
	unitGuid := uuid.New()
	_, err := store.db.ExecContext(ctx,
		`INSERT INTO files (id, filename, file_hash, status) VALUES (1, 'test.tsv', 'hash', 'completed')`)
	require.NoError(t, err)

	_, err = store.db.ExecContext(ctx,
		`INSERT INTO device_data (file_id, unit_guid, line_number) VALUES (1, $1, 1)`, unitGuid)
	require.NoError(t, err)
	_, err = store.db.ExecContext(ctx,
		`INSERT INTO device_data (file_id, unit_guid, line_number) VALUES (1, $1, 2)`, unitGuid)
	require.NoError(t, err)

	count, err := store.CountDeviceDataByUnit(ctx, unitGuid)
	assert.NoError(t, err)
	assert.Equal(t, 2, count)
}

func TestStore_GetStatistics(t *testing.T) {
	store, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()

	// Наполняем данными
	unitGuid := uuid.New()
	_, err := store.db.ExecContext(ctx,
		`INSERT INTO files (id, filename, file_hash, status) VALUES 
		(1, 'a.tsv', 'h1', 'completed'),
		(2, 'b.tsv', 'h2', 'completed')`)
	require.NoError(t, err)

	_, err = store.db.ExecContext(ctx,
		`INSERT INTO device_data (file_id, unit_guid, line_number) VALUES 
		(1, $1, 1), (1, $1, 2), (2, $2, 1)`, uuid.New(), uuid.New())
	require.NoError(t, err)

	_, err = store.db.ExecContext(ctx,
		`INSERT INTO processing_errors (file_id, error_message) VALUES (1, 'err1')`)
	require.NoError(t, err)

	_, err = store.db.ExecContext(ctx,
		`INSERT INTO reports (unit_guid, report_type, file_path) VALUES ($1, 'txt', '/tmp/report.txt')`, unitGuid)
	require.NoError(t, err)

	stats, err := store.GetStatistics(ctx)
	assert.NoError(t, err)

	assert.Equal(t, int64(2), stats["total_files"])
	assert.Equal(t, int64(3), stats["total_device_records"])
	assert.Equal(t, int64(1), stats["total_errors"])
	assert.Equal(t, int64(1), stats["total_reports"])

	filesByStatus, ok := stats["files_by_status"].(map[string]int64)
	assert.True(t, ok)
	assert.Equal(t, int64(2), filesByStatus["completed"])

	recentFiles, ok := stats["recent_files"].([]map[string]interface{})
	assert.True(t, ok)
	assert.Len(t, recentFiles, 2)
}

func TestStore_HealthCheck(t *testing.T) {
	store, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()

	err := store.HealthCheck(ctx)
	assert.NoError(t, err)
}

func TestStore_CheckTablesExist(t *testing.T) {
	store, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()

	err := store.CheckTablesExist(ctx)
	assert.NoError(t, err)
}

func TestStore_Connect(t *testing.T) {
	// Используем уже поднятую БД из setupTestDB, но можем протестировать отдельно
	// Здесь можно использовать testcontainers для проверки функции Connect
	ctx := context.Background()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "postgres:15-alpine",
			ExposedPorts: []string{"5432/tcp"},
			Env: map[string]string{
				"POSTGRES_USER":     "test",
				"POSTGRES_PASSWORD": "test",
				"POSTGRES_DB":       "testdb",
			},
			WaitingFor: wait.ForListeningPort("5432/tcp"),
		},
		Started: true,
	})
	require.NoError(t, err)
	defer container.Terminate(ctx)

	host, _ := container.Host(ctx)
	port, _ := container.MappedPort(ctx, "5432")

	cfg := &config.DatabaseConfig{
		Host:     host,
		Port:     port.Int(),
		User:     "test",
		Password: "test",
		Name:     "testdb",
		SSLMode:  "disable",
	}

	db, err := Connect(cfg)
	assert.NoError(t, err)
	assert.NotNil(t, db)
	db.Close()
}
