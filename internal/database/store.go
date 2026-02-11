// internal/database/store.go
package database

import (
	"TSVProcessingService/db/sqlc"
	"TSVProcessingService/internal/config"
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

// Store - обертка для sqlc с дополнительными методами
type Store struct {
	*sqlc.Queries
	db *sql.DB
}

// NewStore - создание нового хранилища
func NewStore(db *sql.DB) *Store {
	return &Store{
		Queries: sqlc.New(db),
		db:      db,
	}
}

// GetDB возвращает подключение к базе данных
func (s *Store) GetDB() *sql.DB {
	return s.db
}

// Connect - подключение к базе данных
func Connect(cfg *config.DatabaseConfig) (*sql.DB, error) {
	log.Println("🗄️  Connecting to database via sqlc...")

	// Формируем DSN строку
	dsn := cfg.GetDSN()
	log.Printf("  Database: %s", cfg.GetDSNWithoutCredentials())

	// Открываем соединение
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}

	// Настраиваем пул соединений
	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxIdleTime(cfg.MaxIdleTime)
	db.SetConnMaxLifetime(30 * time.Minute)

	// Проверяем соединение
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("database ping failed: %w", err)
	}

	log.Println("  ✓ Database connection established")

	// Выводим статистику
	stats := db.Stats()
	log.Printf("  Pool stats: OpenConnections=%d, InUse=%d, Idle=%d",
		stats.OpenConnections, stats.InUse, stats.Idle)

	return db, nil
}

// Close - закрытие соединения
func (s *Store) Close() error {
	return s.db.Close()
}

// Ping - проверка соединения
func (s *Store) Ping(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

// BeginTransaction - начало транзакции
func (s *Store) BeginTransaction(ctx context.Context) (*sql.Tx, error) {
	return s.db.BeginTx(ctx, nil)
}

// HealthCheck - проверка здоровья базы данных
func (s *Store) HealthCheck(ctx context.Context) error {
	var result int
	query := `SELECT 1`

	err := s.db.QueryRowContext(ctx, query).Scan(&result)
	if err != nil {
		return fmt.Errorf("database health check failed: %w", err)
	}

	if result != 1 {
		return fmt.Errorf("unexpected health check result: %d", result)
	}

	return nil
}

// GetStats - получение статистики соединения
func (s *Store) GetStats() sql.DBStats {
	return s.db.Stats()
}

// CheckTablesExist - проверка существования таблиц
func (s *Store) CheckTablesExist(ctx context.Context) error {
	tables := []string{"files", "device_data", "processing_errors", "reports", "api_logs"}

	for _, table := range tables {
		query := `SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = $1
        )`

		var exists bool
		err := s.db.QueryRowContext(ctx, query, table).Scan(&exists)
		if err != nil {
			return fmt.Errorf("failed to check table %s: %w", table, err)
		}

		if !exists {
			log.Printf("⚠️  Table %s does not exist", table)
			return fmt.Errorf("table %s does not exist", table)
		}
	}

	log.Println("✅ All required tables exist")
	return nil
}

// CountDeviceDataByUnit - подсчет количества записей по unit_guid
func (s *Store) CountDeviceDataByUnit(ctx context.Context, unitGuid uuid.UUID) (int, error) {
	var count int
	query := `SELECT COUNT(*) FROM device_data WHERE unit_guid = $1`
	err := s.db.QueryRowContext(ctx, query, unitGuid).Scan(&count)
	return count, err
}

// GetStatistics возвращает общую статистику по сервису
func (s *Store) GetStatistics(ctx context.Context) (map[string]interface{}, error) {
	stats := make(map[string]interface{})

	// 1. Количество файлов
	var totalFiles int64
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM files`).Scan(&totalFiles)
	if err != nil {
		return nil, fmt.Errorf("failed to count files: %w", err)
	}
	stats["total_files"] = totalFiles

	// 2. Количество обработанных записей
	var totalRecords int64
	err = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM device_data`).Scan(&totalRecords)
	if err != nil {
		return nil, fmt.Errorf("failed to count device_data: %w", err)
	}
	stats["total_device_records"] = totalRecords

	// 3. Количество ошибок обработки
	var totalErrors int64
	err = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM processing_errors`).Scan(&totalErrors)
	if err != nil {
		return nil, fmt.Errorf("failed to count errors: %w", err)
	}
	stats["total_errors"] = totalErrors

	// 4. Количество отчётов
	var totalReports int64
	err = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM reports`).Scan(&totalReports)
	if err != nil {
		return nil, fmt.Errorf("failed to count reports: %w", err)
	}
	stats["total_reports"] = totalReports

	// 5. Статистика по статусам файлов
	rows, err := s.db.QueryContext(ctx, `
        SELECT status, COUNT(*) 
        FROM files 
        GROUP BY status
    `)
	if err != nil {
		return nil, fmt.Errorf("failed to get file status stats: %w", err)
	}
	defer rows.Close()

	fileStats := make(map[string]int64)
	for rows.Next() {
		var status string
		var count int64
		if err := rows.Scan(&status, &count); err == nil {
			fileStats[status] = count
		}
	}
	stats["files_by_status"] = fileStats

	// 6. Последние 5 обработанных файлов
	lastFiles, err := s.db.QueryContext(ctx, `
        SELECT filename, status, created_at 
        FROM files 
        ORDER BY created_at DESC 
        LIMIT 5
    `)
	if err != nil {
		return nil, fmt.Errorf("failed to get recent files: %w", err)
	}
	defer lastFiles.Close()

	recentFiles := make([]map[string]interface{}, 0)
	for lastFiles.Next() {
		var filename, status string
		var createdAt time.Time
		if err := lastFiles.Scan(&filename, &status, &createdAt); err == nil {
			recentFiles = append(recentFiles, map[string]interface{}{
				"filename":   filename,
				"status":     status,
				"created_at": createdAt,
			})
		}
	}
	stats["recent_files"] = recentFiles

	return stats, nil
}
