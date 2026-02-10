// cmd/server/main.go
package main

import (
	"TSVProcessingService/db/sqlc" // исправленный импорт для sqlc
	"TSVProcessingService/internal/config"
	"TSVProcessingService/internal/database"
	"TSVProcessingService/internal/watcher"
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
)

// App - основная структура приложения
type App struct {
	config  *config.AppConfig
	store   *database.Store
	watcher *watcher.Watcher
}

func main() {
	// Инициализация приложения
	app, err := initializeApp()
	if err != nil {
		log.Fatalf("Failed to initialize application: %v", err)
	}

	// Запуск приложения
	if err := app.Run(); err != nil {
		log.Fatalf("Application error: %v", err)
	}
}

// initializeApp - инициализация всех компонентов приложения
func initializeApp() (*App, error) {
	log.Println("🚀 Initializing TSV Processing Service...")

	// 1. Загрузка конфигурации
	cfg, err := config.LoadConfig("")
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %w", err)
	}

	if cfg.IsDebugMode() {
		cfg.PrintConfig()
	}

	// 2. Создание директорий
	if err := createDirectories(cfg); err != nil {
		return nil, fmt.Errorf("failed to create directories: %w", err)
	}

	// 3. Подключение к базе данных через sqlc Store
	db, err := database.Connect(&cfg.Database)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	// Создаем Store
	store := database.NewStore(db)

	// 4. Проверка существования таблиц
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := store.CheckTablesExist(ctx); err != nil {
		log.Printf("Warning: %v", err)
		log.Println("Please run database migrations first")
	}

	// 5. Создание watcher
	watcher := watcher.NewWatcher(
		cfg.Directory.WatchPath,
		cfg.Worker.ScanInterval,
		cfg.Worker.MaxQueueSize,
	)

	// 6. Инициализация структуры приложения
	app := &App{
		config:  cfg,
		store:   store,
		watcher: watcher,
	}

	log.Println("✅ Application initialized successfully")
	return app, nil
}

// createDirectories - создание необходимых директорий
func createDirectories(cfg *config.AppConfig) error {
	log.Println("📁 Creating directories...")

	dirs := []string{
		cfg.Directory.WatchPath,
		cfg.Directory.OutputPath,
		cfg.Directory.ArchivePath,
		cfg.Directory.TempPath,
		"logs",
	}

	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to create directory %s: %w", dir, err)
		}
		log.Printf("  ✓ Created: %s", dir)
	}

	return nil
}

// Run - запуск основного цикла приложения
func (a *App) Run() error {
	log.Println("🚀 Starting application...")

	// Запуск компонентов приложения
	// 1. Запуск мониторинга директории
	go a.startDirectoryWatcher()

	// 2. Запуск воркеров
	go a.startWorkers()

	// 3. Запуск API сервера
	go a.startAPIServer()

	// 4. Запуск health checks
	go a.startHealthChecks()

	// 5. Запуск очистки старых данных
	go a.startCleanupTasks()

	// Ожидание сигнала завершения
	return a.waitForShutdown()
}

// startDirectoryWatcher - запуск мониторинга директории
func (a *App) startDirectoryWatcher() {
	log.Printf("👀 Starting directory watcher for: %s", a.config.Directory.WatchPath)
	go a.watcher.Start()

	// Запускаем обработчик очереди файлов
	go a.processFileQueue()
}

// processFileQueue - обработка файлов из очереди
func (a *App) processFileQueue() {
	log.Println("📂 Starting file queue processor")

	for fileInfo := range a.watcher.GetFileQueue() {
		log.Printf("Processing file: %s (hash: %s)",
			fileInfo.Name, fileInfo.Hash[:8])

		// Обработка файла
		a.processTSVFile(fileInfo)
	}
}

// processTSVFile - обработка TSV файла (используя sqlc)
func (a *App) processTSVFile(fileInfo watcher.FileInfo) {
	log.Printf("Starting processing of: %s", fileInfo.Path)

	ctx := context.Background()

	// 1. Проверяем, не обрабатывался ли уже файл
	existingFile, err := a.store.GetFileByFilename(ctx, fileInfo.Name)
	if err == nil && existingFile.FileHash == fileInfo.Hash {
		log.Printf("File already processed: %s", fileInfo.Name)
		return
	}

	// 2. Создаем запись о файле в базе
	fileParams := sqlc.CreateFileParams{
		Filename: fileInfo.Name,
		FileHash: fileInfo.Hash,
		Status:   sql.NullString{String: "processing", Valid: true},
	}

	file, err := a.store.CreateFile(ctx, fileParams)
	if err != nil {
		log.Printf("Error creating file record: %v", err)
		return
	}

	log.Printf("Created file record with ID: %d", file.ID)

	// 3. Парсим TSV файл (заглушка - нужно реализовать парсер)
	rows, errors := parseTSVFile(fileInfo.Path, file.ID)
	if len(errors) > 0 {
		// Сохраняем ошибки обработки
		for _, processingErr := range errors {
			errParams := sqlc.CreateProcessingErrorParams{
				FileID:       file.ID,
				LineNumber:   processingErr.LineNumber,
				RawLine:      processingErr.RawLine,
				ErrorMessage: processingErr.ErrorMessage,
				FieldName:    processingErr.FieldName,
			}
			_, err := a.store.CreateProcessingError(ctx, errParams)
			if err != nil {
				log.Printf("Error saving processing error: %v", err)
			}
		}
	}

	// 4. Сохраняем данные в базу
	successCount := int32(0)
	failedCount := int32(0)

	for _, row := range rows {
		deviceDataParams := sqlc.CreateDeviceDataParams{
			FileID:     file.ID,
			UnitGuid:   row.UnitGuid,
			Mqtt:       row.Mqtt,
			Invid:      row.Invid,
			MsgID:      row.MsgID,
			Text:       row.Text,
			Context:    row.Context,
			Class:      row.Class,
			Level:      row.Level,
			Area:       row.Area,
			Addr:       row.Addr,
			Block:      row.Block,
			Type:       row.Type,
			Bit:        row.Bit,
			InvertBit:  row.InvertBit,
			LineNumber: row.LineNumber,
		}

		_, err := a.store.CreateDeviceData(ctx, deviceDataParams)
		if err != nil {
			log.Printf("Error saving device data: %v", err)
			failedCount++
			continue
		}
		successCount++
	}

	// 5. Обновляем статус файла
	updateParams := sqlc.UpdateFileProgressParams{
		ID:            file.ID,
		RowsProcessed: sql.NullInt32{Int32: successCount, Valid: true},
		RowsFailed:    sql.NullInt32{Int32: failedCount, Valid: true},
	}

	_, err = a.store.UpdateFileProgress(ctx, updateParams)
	if err != nil {
		log.Printf("Error updating file progress: %v", err)
		return
	}

	// 6. Устанавливаем финальный статус
	status := "completed"
	if failedCount > 0 && failedCount == int32(len(rows)) {
		status = "failed"
	} else if failedCount > 0 {
		status = "partial"
	}

	statusParams := sqlc.UpdateFileStatusParams{
		ID:     file.ID,
		Status: sql.NullString{String: status, Valid: true},
	}

	_, err = a.store.UpdateFileStatus(ctx, statusParams)
	if err != nil {
		log.Printf("Error updating file status: %v", err)
	}

	log.Printf("Finished processing: %s. Success: %d, Failed: %d",
		fileInfo.Name, successCount, failedCount)
}

// startWorkers - запуск воркеров
func (a *App) startWorkers() {
	log.Printf("👷 Starting %d workers", a.config.Worker.MaxWorkers)
	// TODO: Реализация воркеров для параллельной обработки
}

// startAPIServer - запуск API сервера
func (a *App) startAPIServer() {
	addr := a.config.Server.GetListenAddr()
	log.Printf("🌐 Starting API server on %s", addr)
	// TODO: Реализация API сервера с использованием sqlc queries
}

// startHealthChecks - запуск health checks
func (a *App) startHealthChecks() {
	log.Println("🏥 Starting health checks...")

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		// Проверка базы данных
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := a.store.HealthCheck(ctx)
		cancel()

		if err != nil {
			log.Printf("⚠️  Database health check failed: %v", err)
		} else {
			log.Printf("✅ Database health check passed")
		}

		// Вывод статистики базы данных
		stats := a.store.GetStats()
		log.Printf("📊 DB Stats: OpenConnections=%d, InUse=%d, Idle=%d",
			stats.OpenConnections, stats.InUse, stats.Idle)
	}
}

// startCleanupTasks - запуск задач очистки
func (a *App) startCleanupTasks() {
	log.Println("🧹 Starting cleanup tasks...")

	ticker := time.NewTicker(24 * time.Hour) // Ежедневно
	defer ticker.Stop()

	// Запускаем сразу при старте
	go a.runCleanup()

	for range ticker.C {
		go a.runCleanup()
	}
}

// runCleanup - выполнение задач очистки
func (a *App) runCleanup() {
	ctx := context.Background()

	// Очистка старых API логов (30 дней)
	err := a.store.CleanupOldApiLogs(ctx)
	if err != nil {
		log.Printf("Error cleaning old API logs: %v", err)
	}

	// Очистка старых файлов
	err = a.store.DeleteOldFiles(ctx, sql.NullString{String: "completed", Valid: true})
	if err != nil {
		log.Printf("Error cleaning old files: %v", err)
	}

	// Очистка старых отчетов (1 год)
	err = a.store.DeleteOldReports(ctx)
	if err != nil {
		log.Printf("Error cleaning old reports: %v", err)
	}

	log.Println("✅ Cleanup tasks completed")
}

// waitForShutdown - ожидание сигнала завершения
func (a *App) waitForShutdown() error {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigChan
	log.Printf("🛑 Received signal %v, shutting down...", sig)

	return a.shutdown()
}

// shutdown - graceful shutdown приложения
func (a *App) shutdown() error {
	log.Println("🔒 Shutting down application...")

	// 1. Остановка watcher
	if a.watcher != nil {
		a.watcher.Stop()
		log.Println("  ✓ Directory watcher stopped")
	}

	// 2. Закрытие соединения с базой данных
	if a.store != nil {
		if err := a.store.Close(); err != nil {
			log.Printf("  Error closing database: %v", err)
		} else {
			log.Println("  ✓ Database connection closed")
		}
	}

	log.Println("👋 Application shutdown complete")
	return nil
}

// Структуры для парсинга TSV (заглушка)
type TSVRow struct {
	UnitGuid   uuid.UUID
	Mqtt       sql.NullString
	Invid      sql.NullString
	MsgID      sql.NullString
	Text       sql.NullString
	Context    sql.NullString
	Class      sql.NullString
	Level      sql.NullInt32
	Area       sql.NullString
	Addr       sql.NullString
	Block      sql.NullString
	Type       sql.NullString
	Bit        sql.NullInt32
	InvertBit  sql.NullBool
	LineNumber int32
}

type ProcessingError struct {
	LineNumber   sql.NullInt32
	RawLine      sql.NullString
	ErrorMessage string
	FieldName    sql.NullString
}

func parseTSVFile(filePath string, fileID int64) ([]TSVRow, []ProcessingError) {
	// TODO: Реализовать парсинг TSV файла
	// Временная заглушка
	return []TSVRow{}, []ProcessingError{}
}
