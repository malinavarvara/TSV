package processor

import (
	"TSVProcessingService/db/sqlc"
	"TSVProcessingService/internal/config"
	"TSVProcessingService/internal/watcher"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

func setupTestDB(t *testing.T) *sql.DB {
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
	return db
}

func setupTestProcessor(t *testing.T) (*Processor, *sql.DB, *config.DirectoryConfig, func()) {
	db := setupTestDB(t)
	queries := sqlc.New(db) // <-- СОЗДАЁМ РЕАЛЬНЫЙ sqlc.Queries

	tmpDir, err := os.MkdirTemp("", "processor_test_*")
	require.NoError(t, err)

	cfg := &config.DirectoryConfig{
		WatchPath:   filepath.Join(tmpDir, "incoming"),
		OutputPath:  filepath.Join(tmpDir, "reports"),
		ArchivePath: filepath.Join(tmpDir, "archive"),
		ErrorPath:   filepath.Join(tmpDir, "errors"),
		TempPath:    filepath.Join(tmpDir, "tmp"),
	}
	for _, dir := range []string{cfg.WatchPath, cfg.OutputPath, cfg.ArchivePath, cfg.ErrorPath} {
		err := os.MkdirAll(dir, 0755)
		require.NoError(t, err)
	}

	processor := NewProcessor(db, queries, cfg) // <-- ИСПОЛЬЗУЕМ КОНСТРУКТОР

	cleanup := func() {
		db.Close()
		os.RemoveAll(tmpDir)
	}
	return processor, db, cfg, cleanup
}

func createTestTSV(t *testing.T, dir, filename string, lines []string) string {
	path := filepath.Join(dir, filename)
	f, err := os.Create(path)
	require.NoError(t, err)
	defer f.Close()
	for _, line := range lines {
		_, err := f.WriteString(line + "\n")
		require.NoError(t, err)
	}
	return path
}

func calculateFileHash(filePath string) (string, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer f.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

// ---------- parseLine ----------
func TestParseLine_Valid(t *testing.T) {
	p := &Processor{}
	fields := []string{
		"1", "", "G-044322", "01749246-95f6-57db-b7c3-2ae0e8be671f",
		"cold7_Defrost_status", "Разморозка", "", "waiting", "100", "LOCAL",
		"cold7_status.Defrost_status", "", "", "", "",
	}
	row, err := p.parseLine(fields, 1)
	assert.NoError(t, err)
	assert.Equal(t, uuid.MustParse("01749246-95f6-57db-b7c3-2ae0e8be671f"), row.UnitGuid)
	assert.Equal(t, "G-044322", row.Invid.String)
}

func TestParseLine_InvalidUUID(t *testing.T) {
	p := &Processor{}
	fields := []string{"1", "", "G-044322", "not-a-uuid"}
	_, err := p.parseLine(fields, 1)
	assert.ErrorContains(t, err, "invalid unit_guid")
}

func TestParseLine_InvalidClass(t *testing.T) {
	p := &Processor{}
	fields := []string{
		"1", "", "G-044322", "01749246-95f6-57db-b7c3-2ae0e8be671f",
		"msg", "text", "", "INVALID_CLASS", "100",
	}
	_, err := p.parseLine(fields, 1)
	assert.ErrorContains(t, err, "invalid class value")
}

func TestParseLine_InvalidLevel(t *testing.T) {
	p := &Processor{}
	fields := []string{
		"1", "", "G-044322", "01749246-95f6-57db-b7c3-2ae0e8be671f",
		"msg", "text", "", "alarm", "abc",
	}
	_, err := p.parseLine(fields, 1)
	assert.ErrorContains(t, err, "invalid level (not integer)")
}

func TestParseLine_InvalidBit(t *testing.T) {
	p := &Processor{}
	fields := []string{
		"1", "", "G-044322", "01749246-95f6-57db-b7c3-2ae0e8be671f",
		"msg", "text", "", "alarm", "100", "LOCAL", "addr", "", "coil", "not-a-number",
	}
	_, err := p.parseLine(fields, 1)
	assert.ErrorContains(t, err, "invalid bit (not integer)")
}

func TestParseLine_InvalidInvertBit(t *testing.T) {
	p := &Processor{}
	fields := []string{
		"1", "", "G-044322", "01749246-95f6-57db-b7c3-2ae0e8be671f",
		"msg", "text", "", "alarm", "100", "LOCAL", "addr", "", "coil", "1", "maybe",
	}
	_, err := p.parseLine(fields, 1)
	assert.ErrorContains(t, err, "invalid invert_bit")
}

// ---------- parseTSVFile ----------
func TestParseTSVFile_ValidFile(t *testing.T) {
	_, _, cfg, cleanup := setupTestProcessor(t)
	defer cleanup()

	lines := []string{
		"n\tmqtt\tinvid\tunit_guid\tmsg_id\ttext\tcontext\tclass\tlevel\tarea\taddr\tblock\ttype\tbit\tinvert_bit",
		"1\t\tG-044322\t01749246-95f6-57db-b7c3-2ae0e8be671f\tcold7_Defrost_status\tРазморозка\t\twaiting\t100\tLOCAL\tcold7_status.Defrost_status\t\t\t\t",
		"2\t\tG-044322\t01749246-95f6-57db-b7c3-2ae0e8be671f\tcold7_VentSK_status\tВентилятор\t\tworking\t100\tLOCAL\tcold7_status.VentSK_status\t\t\t\t",
	}
	path := createTestTSV(t, cfg.WatchPath, "valid.tsv", lines)

	p, _, _, _ := setupTestProcessor(t)
	rows, errors := p.parseTSVFile(path, 1)

	assert.Len(t, rows, 2)
	assert.Len(t, errors, 0)
	assert.Equal(t, "cold7_Defrost_status", rows[0].MsgID.String)
	assert.Equal(t, "cold7_VentSK_status", rows[1].MsgID.String)
}

func TestParseTSVFile_WithErrors(t *testing.T) {
	_, _, cfg, cleanup := setupTestProcessor(t)
	defer cleanup()

	lines := []string{
		"n\tmqtt\tinvid\tunit_guid\tmsg_id\ttext\tcontext\tclass\tlevel\tarea\taddr\tblock\ttype\tbit\tinvert_bit",
		"1\t\tG-044322\tinvalid-uuid\tcold7_Defrost_status\tРазморозка\t\twaiting\t100\tLOCAL\taddr\t\t\t\t",
		"2\t\tG-044322\t01749246-95f6-57db-b7c3-2ae0e8be671f\tbad_level\tтекст\t\talarm\tnot_int\tLOCAL\taddr\t\t\t\t",
		"3\t\tG-044322\t01749246-95f6-57db-b7c3-2ae0e8be671f\tbad_class\tтекст\t\tinvalid_class\t100\tLOCAL\taddr\t\t\t\t",
	}
	path := createTestTSV(t, cfg.WatchPath, "with_errors.tsv", lines)

	p, _, _, _ := setupTestProcessor(t)
	rows, errors := p.parseTSVFile(path, 1)

	assert.Len(t, rows, 0)
	assert.Len(t, errors, 3)
	assert.Contains(t, errors[0].ErrorMessage, "invalid unit_guid")
	assert.Contains(t, errors[1].ErrorMessage, "invalid level")
	assert.Contains(t, errors[2].ErrorMessage, "invalid class value")
}

// ---------- ProcessFile ----------
func TestProcessFile_Success(t *testing.T) {
	processor, db, cfg, cleanup := setupTestProcessor(t)
	defer cleanup()

	lines := []string{
		"n\tmqtt\tinvid\tunit_guid\tmsg_id\ttext\tcontext\tclass\tlevel\tarea\taddr\tblock\ttype\tbit\tinvert_bit",
		"1\t\tG-044322\t01749246-95f6-57db-b7c3-2ae0e8be671f\tcold7_Defrost_status\tРазморозка\t\twaiting\t100\tLOCAL\tcold7_status.Defrost_status\t\t\t\t",
		"2\t\tG-044322\t01749246-95f6-57db-b7c3-2ae0e8be671f\tcold7_VentSK_status\tВентилятор\t\tworking\t100\tLOCAL\tcold7_status.VentSK_status\t\t\t\t",
	}
	filePath := createTestTSV(t, cfg.WatchPath, "test_success.tsv", lines)

	hash, err := calculateFileHash(filePath)
	require.NoError(t, err)

	fileInfo := watcher.FileInfo{
		Path: filePath,
		Name: "test_success.tsv",
		Hash: hash,
	}

	ctx := context.Background()
	err = processor.ProcessFile(ctx, fileInfo)
	require.NoError(t, err)

	// файл должен быть перемещён в архив
	_, err = os.Stat(filepath.Join(cfg.ArchivePath, "test_success.tsv"))
	assert.NoError(t, err)
	_, err = os.Stat(filePath)
	assert.True(t, os.IsNotExist(err))

	// проверка записей в БД
	var fileID int64
	err = db.QueryRow(`SELECT id FROM files WHERE filename = ?`, "test_success.tsv").Scan(&fileID)
	require.NoError(t, err)

	var count int
	err = db.QueryRow(`SELECT COUNT(*) FROM device_data WHERE file_id = ?`, fileID).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 2, count)

	var status string
	err = db.QueryRow(`SELECT status FROM files WHERE id = ?`, fileID).Scan(&status)
	require.NoError(t, err)
	assert.Equal(t, "completed", status)

	guid := uuid.MustParse("01749246-95f6-57db-b7c3-2ae0e8be671f")
	var reportCount int
	err = db.QueryRow(`SELECT COUNT(*) FROM reports WHERE unit_guid = ?`, guid.String()).Scan(&reportCount)
	require.NoError(t, err)
	assert.Equal(t, 1, reportCount)

	var reportPath string
	err = db.QueryRow(`SELECT file_path FROM reports WHERE unit_guid = ?`, guid.String()).Scan(&reportPath)
	require.NoError(t, err)
	_, err = os.Stat(reportPath)
	assert.NoError(t, err)
}

func TestProcessFile_AlreadyProcessed(t *testing.T) {
	processor, db, cfg, cleanup := setupTestProcessor(t)
	defer cleanup()

	lines := []string{
		"n\tmqtt\tinvid\tunit_guid\tmsg_id\ttext\tcontext\tclass\tlevel\tarea\taddr\tblock\ttype\tbit\tinvert_bit",
		"1\t\tG-044322\t01749246-95f6-57db-b7c3-2ae0e8be671f\tmsg\ttext\t\talarm\t100\tLOCAL\taddr\t\t\t\t",
	}
	filePath := createTestTSV(t, cfg.WatchPath, "already.tsv", lines)
	hash, _ := calculateFileHash(filePath)
	fileInfo := watcher.FileInfo{Path: filePath, Name: "already.tsv", Hash: hash}

	ctx := context.Background()
	err := processor.ProcessFile(ctx, fileInfo)
	require.NoError(t, err)

	err = processor.ProcessFile(ctx, fileInfo)
	require.NoError(t, err)

	var count int
	err = db.QueryRow(`SELECT COUNT(*) FROM files WHERE filename = ?`, "already.tsv").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestProcessFile_InvalidFile(t *testing.T) {
	processor, db, cfg, cleanup := setupTestProcessor(t)
	defer cleanup()

	lines := []string{
		"n\tmqtt\tinvid\tunit_guid\tmsg_id\ttext\tcontext\tclass\tlevel\tarea\taddr\tblock\ttype\tbit\tinvert_bit",
		"1\t\tG-044322\tnot-a-uuid\tmsg\ttext\t\talarm\t100\tLOCAL\taddr\t\t\t\t",
	}
	filePath := createTestTSV(t, cfg.WatchPath, "invalid.tsv", lines)
	hash, _ := calculateFileHash(filePath)
	fileInfo := watcher.FileInfo{Path: filePath, Name: "invalid.tsv", Hash: hash}

	ctx := context.Background()
	err := processor.ProcessFile(ctx, fileInfo)
	require.NoError(t, err)

	_, err = os.Stat(filepath.Join(cfg.ErrorPath, "invalid.tsv"))
	assert.NoError(t, err)

	var status string
	err = db.QueryRow(`SELECT status FROM files WHERE filename = ?`, "invalid.tsv").Scan(&status)
	require.NoError(t, err)
	assert.Equal(t, "failed", status)

	var errorCount int
	err = db.QueryRow(`SELECT COUNT(*) FROM processing_errors WHERE file_id = (SELECT id FROM files WHERE filename = ?)`, "invalid.tsv").Scan(&errorCount)
	require.NoError(t, err)
	assert.Greater(t, errorCount, 0)
}
