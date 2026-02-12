package database

import (
	"context"
	"database/sql"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite" // driver name "sqlite"
)

func setupTestStore(t *testing.T) (*Store, func()) {
	// ВАЖНО: используем ":memory:" – уникальная БД на каждое соединение
	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)

	schema := `
	CREATE TABLE files (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		filename TEXT UNIQUE NOT NULL,
		file_hash TEXT NOT NULL,
		status TEXT DEFAULT 'pending',
		rows_processed INTEGER DEFAULT 0,
		rows_failed INTEGER DEFAULT 0,
		error_message TEXT,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
		updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
	);
	CREATE TABLE device_data (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		file_id INTEGER NOT NULL,
		unit_guid TEXT NOT NULL,
		mqtt TEXT,
		invid TEXT,
		msg_id TEXT,
		text TEXT,
		context TEXT,
		class TEXT,
		level INTEGER,
		area TEXT,
		addr TEXT,
		block TEXT,
		type TEXT,
		bit INTEGER,
		invert_bit INTEGER DEFAULT 0,
		line_number INTEGER NOT NULL,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
		FOREIGN KEY (file_id) REFERENCES files(id) ON DELETE CASCADE
	);
	CREATE TABLE processing_errors (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		file_id INTEGER NOT NULL,
		line_number INTEGER,
		raw_line TEXT,
		error_message TEXT NOT NULL,
		field_name TEXT,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
		FOREIGN KEY (file_id) REFERENCES files(id) ON DELETE CASCADE
	);
	CREATE TABLE reports (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		unit_guid TEXT NOT NULL,
		report_type TEXT DEFAULT 'pdf',
		file_path TEXT NOT NULL,
		generated_at DATETIME DEFAULT CURRENT_TIMESTAMP
	);
	`
	_, err = db.Exec(schema)
	require.NoError(t, err)

	store := NewStore(db)
	cleanup := func() {
		db.Close()
	}
	return store, cleanup
}

// insertTestData — наполнение БД тестовыми данными
func insertTestData(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`
		INSERT INTO files (filename, file_hash, status) VALUES 
		('test1.tsv', 'hash1', 'completed'),
		('test2.tsv', 'hash2', 'failed')
	`)
	require.NoError(t, err)

	var fileID1, fileID2 int64
	err = db.QueryRow(`SELECT id FROM files WHERE filename = 'test1.tsv'`).Scan(&fileID1)
	require.NoError(t, err)
	err = db.QueryRow(`SELECT id FROM files WHERE filename = 'test2.tsv'`).Scan(&fileID2)
	require.NoError(t, err)

	guid1 := uuid.New().String()
	guid2 := uuid.New().String()

	_, err = db.Exec(`
		INSERT INTO device_data (file_id, unit_guid, invid, msg_id, text, class, level, line_number) VALUES 
		(?, ?, 'INV001', 'msg1', 'text1', 'alarm', 100, 1),
		(?, ?, 'INV001', 'msg2', 'text2', 'warning', 50, 2)
	`, fileID1, guid1, fileID1, guid1)
	require.NoError(t, err)

	_, err = db.Exec(`
		INSERT INTO device_data (file_id, unit_guid, invid, msg_id, text, class, level, line_number) VALUES 
		(?, ?, 'INV002', 'msg3', 'text3', 'info', 0, 1)
	`, fileID2, guid2)
	require.NoError(t, err)

	_, err = db.Exec(`
		INSERT INTO processing_errors (file_id, line_number, raw_line, error_message) VALUES 
		(?, 1, 'bad line', 'invalid uuid')
	`, fileID2)
	require.NoError(t, err)

	_, err = db.Exec(`
		INSERT INTO reports (unit_guid, report_type, file_path) VALUES 
		(?, 'pdf', '/reports/report1.pdf')
	`, guid1)
	require.NoError(t, err)
}

func TestCountDeviceDataByUnit(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	guid := uuid.New()
	count, err := store.CountDeviceDataByUnit(ctx, guid)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	insertTestData(t, store.db)

	var guidStr string
	err = store.db.QueryRow(`SELECT unit_guid FROM device_data WHERE invid = 'INV001' LIMIT 1`).Scan(&guidStr)
	require.NoError(t, err)
	unitGuid := uuid.MustParse(guidStr)

	count, err = store.CountDeviceDataByUnit(ctx, unitGuid)
	require.NoError(t, err)
	assert.Equal(t, 2, count)
}

func TestGetStatistics(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	stats, err := store.GetStatistics(ctx)
	require.NoError(t, err)

	assert.EqualValues(t, 0, stats["total_files"])
	assert.EqualValues(t, 0, stats["total_device_records"])
	assert.EqualValues(t, 0, stats["total_errors"])
	assert.EqualValues(t, 0, stats["total_reports"])
	assert.NotNil(t, stats["files_by_status"])
	assert.Empty(t, stats["recent_files"])

	insertTestData(t, store.db)

	stats, err = store.GetStatistics(ctx)
	require.NoError(t, err)

	assert.EqualValues(t, 2, stats["total_files"])
	assert.EqualValues(t, 3, stats["total_device_records"])
	assert.EqualValues(t, 1, stats["total_errors"])
	assert.EqualValues(t, 1, stats["total_reports"])

	filesByStatus := stats["files_by_status"].(map[string]int64)
	assert.EqualValues(t, 1, filesByStatus["completed"])
	assert.EqualValues(t, 1, filesByStatus["failed"])

	recentFiles := stats["recent_files"].([]map[string]interface{})
	assert.Len(t, recentFiles, 2)
}
