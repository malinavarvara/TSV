// cmd/server/main.go
package main

import (
	"TSVProcessingService/db/sqlc"
	"TSVProcessingService/internal/config"
	"TSVProcessingService/internal/database"
	"TSVProcessingService/internal/processor"
	"TSVProcessingService/internal/watcher"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
)

// App - основная структура приложения
type App struct {
	config    *config.AppConfig
	store     *database.Store
	queries   *sqlc.Queries
	watcher   *watcher.Watcher
	processor *processor.Processor
	router    *mux.Router
	server    *http.Server
	workerWg  sync.WaitGroup
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

	// 3. Подключение к базе данных
	db, err := database.Connect(&cfg.Database)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	// Создаем Store
	store := database.NewStore(db)
	queries := sqlc.New(db)

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

	// 6. Создание processor
	processor := processor.NewProcessor(queries, &cfg.Directory)

	// 7. Инициализация структуры приложения
	app := &App{
		config:    cfg,
		store:     store,
		queries:   queries,
		watcher:   watcher,
		processor: processor,
		router:    mux.NewRouter(),
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
	// Запускаем watcher (он сам наполняет очередь)
	go a.watcher.Start()
}

// startWorkers - запуск пула воркеров для параллельной обработки файлов
func (a *App) startWorkers() {
	log.Printf("👷 Starting %d workers", a.config.Worker.MaxWorkers)

	fileQueue := a.watcher.GetFileQueue()

	// Запускаем указанное количество воркеров
	for i := 0; i < a.config.Worker.MaxWorkers; i++ {
		a.workerWg.Add(1)
		go a.worker(i+1, fileQueue)
	}
}

// worker - отдельный воркер, обрабатывающий файлы из очереди
func (a *App) worker(id int, fileQueue <-chan watcher.FileInfo) {
	defer a.workerWg.Done()
	log.Printf("  👤 Worker %d started", id)

	for fileInfo := range fileQueue {
		log.Printf("Worker %d: processing file: %s (hash: %s)",
			id, fileInfo.Name, fileInfo.Hash[:8])

		// Обработка файла через processor
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		err := a.processor.ProcessFile(ctx, fileInfo)
		cancel()

		if err != nil {
			log.Printf("Worker %d: error processing file %s: %v",
				id, fileInfo.Name, err)
		} else {
			log.Printf("Worker %d: completed file %s", id, fileInfo.Name)
		}
	}

	log.Printf("  👤 Worker %d stopped (queue closed)", id)
}

// startAPIServer - запуск API сервера
func (a *App) startAPIServer() {
	addr := a.config.Server.GetListenAddr()
	log.Printf("🌐 Starting API server on %s", addr)

	// Настраиваем маршруты
	a.setupRoutes()

	// Создаем HTTP сервер
	a.server = &http.Server{
		Addr:         addr,
		Handler:      a.router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// Запускаем сервер в горутине
	go func() {
		if err := a.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("❌ Failed to start API server: %v", err)
		}
	}()
}

// setupRoutes - настройка маршрутов API
func (a *App) setupRoutes() {
	// Health check
	a.router.HandleFunc("/health", a.healthCheck).Methods("GET")

	// API v1
	v1 := a.router.PathPrefix("/api/v1").Subrouter()

	// Device data endpoints
	v1.HandleFunc("/devices/{unit_guid}/data", a.getDeviceData).Methods("GET")

	// File endpoints
	v1.HandleFunc("/files", a.getFiles).Methods("GET")
	v1.HandleFunc("/files/{filename}", a.getFileStatus).Methods("GET")
	v1.HandleFunc("/files/{filename}/errors", a.getFileErrors).Methods("GET")
	v1.HandleFunc("/files/{filename}/process", a.processFile).Methods("POST")

	// Report endpoints
	v1.HandleFunc("/reports/{unit_guid}", a.getReports).Methods("GET")
	v1.HandleFunc("/reports/{unit_guid}/generate", a.generateReport).Methods("POST")

	// Statistics endpoints
	v1.HandleFunc("/statistics", a.getStatistics).Methods("GET")
}

// healthCheck - обработчик health check
func (a *App) healthCheck(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	// Проверяем соединение с БД
	if err := a.store.HealthCheck(ctx); err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]string{
			"status":  "unhealthy",
			"message": "Database connection failed",
		})
		return
	}

	json.NewEncoder(w).Encode(map[string]string{
		"status":  "healthy",
		"message": "Service is running",
	})
}

// getDeviceData - получение данных устройства
func (a *App) getDeviceData(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	unitGuidStr := vars["unit_guid"]

	// Парсим unit_guid
	unitGuid, err := uuid.Parse(unitGuidStr)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{
			"error": "Invalid unit_guid format",
		})
		return
	}

	// Парсим параметры пагинации
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	if page < 1 {
		page = 1
	}

	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit < 1 || limit > 100 {
		limit = 50
	}

	offset := (page - 1) * limit

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	// Получаем данные из БД
	params := sqlc.ListDeviceDataByUnitParams{
		UnitGuid: unitGuid,
		Limit:    int32(limit),
		Offset:   int32(offset),
	}

	data, err := a.queries.ListDeviceDataByUnit(ctx, params)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{
			"error": "Failed to fetch device data",
		})
		return
	}

	// Получаем общее количество с помощью нового метода
	total, err := a.store.CountDeviceDataByUnit(ctx, unitGuid)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{
			"error": "Failed to count device data",
		})
		return
	}

	response := map[string]interface{}{
		"data": data,
		"pagination": map[string]interface{}{
			"page":  page,
			"limit": limit,
			"total": total,
		},
	}

	json.NewEncoder(w).Encode(response)
}

// getFiles - получение списка файлов
func (a *App) getFiles(w http.ResponseWriter, r *http.Request) {
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	if page < 1 {
		page = 1
	}

	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit < 1 || limit > 100 {
		limit = 20
	}

	offset := (page - 1) * limit

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	params := sqlc.ListFilesParams{
		Limit:  int32(limit),
		Offset: int32(offset),
	}

	files, err := a.queries.ListFiles(ctx, params)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{
			"error": "Failed to fetch files",
		})
		return
	}

	json.NewEncoder(w).Encode(files)
}

// getFileStatus - получение статуса файла
func (a *App) getFileStatus(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	filename := vars["filename"]

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	file, err := a.queries.GetFileByFilename(ctx, filename)
	if err != nil {
		if err == sql.ErrNoRows {
			w.WriteHeader(http.StatusNotFound)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
		json.NewEncoder(w).Encode(map[string]string{
			"error": "File not found",
		})
		return
	}

	json.NewEncoder(w).Encode(file)
}

// getFileErrors - получение ошибок обработки файла
func (a *App) getFileErrors(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	filename := vars["filename"]

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	file, err := a.queries.GetFileByFilename(ctx, filename)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{
			"error": "File not found",
		})
		return
	}

	errors, err := a.queries.ListProcessingErrorsByFile(ctx, file.ID)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{
			"error": "Failed to fetch errors",
		})
		return
	}

	json.NewEncoder(w).Encode(errors)
}

// processFile - обработка файла по запросу API
func (a *App) processFile(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	filename := vars["filename"]

	filePath := filepath.Join(a.config.Directory.WatchPath, filename)

	// Проверяем существование файла
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{
			"error": "File not found",
		})
		return
	}

	// Создаем FileInfo для processor
	fileInfo := watcher.FileInfo{
		Name: filename,
		Path: filePath,
		Hash: "", // В реальности нужно вычислить хеш
		Size: 0,  // В реальности нужно получить размер
	}

	// Обрабатываем файл в горутине
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()

		if err := a.processor.ProcessFile(ctx, fileInfo); err != nil {
			log.Printf("API processing error for %s: %v", filename, err)
		}
	}()

	json.NewEncoder(w).Encode(map[string]string{
		"message": "File processing started",
	})
}

// getReports - получение отчетов по устройству
func (a *App) getReports(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	unitGuidStr := vars["unit_guid"]

	unitGuid, err := uuid.Parse(unitGuidStr)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{
			"error": "Invalid unit_guid format",
		})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	reports, err := a.queries.GetReportsByUnit(ctx, unitGuid)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{
			"error": "Failed to fetch reports",
		})
		return
	}

	json.NewEncoder(w).Encode(reports)
}

// generateReport - генерация отчета для устройства
func (a *App) generateReport(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	unitGuidStr := vars["unit_guid"]

	unitGuid, err := uuid.Parse(unitGuidStr)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{
			"error": "Invalid unit_guid format",
		})
		return
	}

	// TODO: Реализовать генерацию отчета через processor

	json.NewEncoder(w).Encode(map[string]string{
		"message":   "Report generation started",
		"unit_guid": unitGuid.String(),
	})
}

// getStatistics - получение статистики
func (a *App) getStatistics(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	stats, err := a.queries.GetApiStatistics(ctx)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{
			"error": "Failed to fetch statistics",
		})
		return
	}

	json.NewEncoder(w).Encode(stats)
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
	err := a.queries.CleanupOldApiLogs(ctx)
	if err != nil {
		log.Printf("Error cleaning old API logs: %v", err)
	}

	// Очистка старых файлов
	err = a.queries.DeleteOldFiles(ctx, sql.NullString{String: "completed", Valid: true})
	if err != nil {
		log.Printf("Error cleaning old files: %v", err)
	}

	// Очистка старых отчетов (1 год)
	err = a.queries.DeleteOldReports(ctx)
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

	// 1. Остановка API сервера
	if a.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if err := a.server.Shutdown(ctx); err != nil {
			log.Printf("Error shutting down API server: %v", err)
		} else {
			log.Println("  ✓ API server stopped")
		}
	}

	// 2. Остановка watcher
	if a.watcher != nil {
		a.watcher.Stop()
		log.Println("  ✓ Directory watcher stopped")
	}

	// 3. Ожидаем завершения всех воркеров (с таймаутом)
	log.Println("  ⏳ Waiting for workers to finish current tasks...")
	waitChan := make(chan struct{})
	go func() {
		a.workerWg.Wait()
		close(waitChan)
	}()
	select {
	case <-waitChan:
		log.Println("  ✓ All workers stopped")
	case <-time.After(30 * time.Second):
		log.Println("  ⚠️ Worker shutdown timeout (some tasks may be incomplete)")
	}

	// 4. Закрытие соединения с базой данных
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
