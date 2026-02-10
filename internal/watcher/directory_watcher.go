// internal/watcher/directory_watcher.go
package watcher

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// FileInfo - информация о файле
type FileInfo struct {
	Path    string
	Name    string
	Size    int64
	ModTime time.Time
	Hash    string
}

// Watcher - монитор директории
type Watcher struct {
	watchDir  string
	interval  time.Duration
	fileQueue chan FileInfo
	processed map[string]bool
	stopChan  chan struct{}
	mu        sync.RWMutex // Добавьте это поле
}

// NewWatcher - создание нового watcher
func NewWatcher(watchDir string, interval time.Duration, queueSize int) *Watcher {
	return &Watcher{
		watchDir:  watchDir,
		interval:  interval,
		fileQueue: make(chan FileInfo, queueSize),
		processed: make(map[string]bool),
		stopChan:  make(chan struct{}),
	}
}

// Start - запуск мониторинга
func (w *Watcher) Start() {
	log.Printf("Starting directory watcher for: %s (interval: %v)", w.watchDir, w.interval)

	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	// Первоначальное сканирование
	w.scanDirectory()

	// Периодическое сканирование
	for {
		select {
		case <-ticker.C:
			w.scanDirectory()
		case <-w.stopChan:
			log.Println("Directory watcher stopped")
			return
		}
	}
}

// Stop - остановка мониторинга
func (w *Watcher) Stop() {
	close(w.stopChan)
}

// GetFileQueue - получение канала с файлами
func (w *Watcher) GetFileQueue() <-chan FileInfo {
	return w.fileQueue
}

// scanDirectory - сканирование директории
func (w *Watcher) scanDirectory() {
	files, err := os.ReadDir(w.watchDir)
	if err != nil {
		log.Printf("Error reading directory %s: %v", w.watchDir, err)
		return
	}

	for _, file := range files {
		// Пропускаем директории и скрытые файлы
		if file.IsDir() || strings.HasPrefix(file.Name(), ".") {
			continue
		}

		// Проверяем расширение .tsv
		if !strings.HasSuffix(strings.ToLower(file.Name()), ".tsv") {
			continue
		}

		filePath := filepath.Join(w.watchDir, file.Name())
		w.processFile(filePath)
	}
}

// processFile - обработка файла
func (w *Watcher) processFile(filePath string) {
	// Получаем информацию о файле
	info, err := os.Stat(filePath)
	if err != nil {
		log.Printf("Error getting file info %s: %v", filePath, err)
		return
	}

	// Вычисляем хэш файла (для предотвращения повторной обработки)
	hash, err := w.calculateFileHash(filePath)
	if err != nil {
		log.Printf("Error calculating hash for %s: %v", filePath, err)
		return
	}

	// Проверяем, не обрабатывался ли уже этот файл
	if w.isProcessed(hash) {
		log.Printf("File already processed: %s (hash: %s)", filePath, hash[:8])
		return
	}

	// Создаем FileInfo
	fileInfo := FileInfo{
		Path:    filePath,
		Name:    info.Name(),
		Size:    info.Size(),
		ModTime: info.ModTime(),
		Hash:    hash,
	}

	// Добавляем в очередь (неблокирующая попытка)
	select {
	case w.fileQueue <- fileInfo:
		w.markAsProcessed(hash)
		log.Printf("Queued file for processing: %s (size: %d bytes)", filePath, info.Size())
	default:
		log.Printf("Queue is full, skipping file: %s", filePath)
	}
}

// calculateFileHash - вычисление хэша файла
func (w *Watcher) calculateFileHash(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}

	return hex.EncodeToString(hash.Sum(nil)), nil
}

// isProcessed - проверка, обработан ли файл
func (w *Watcher) isProcessed(hash string) bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.processed[hash]
}

// markAsProcessed - пометить файл как обработанный
func (w *Watcher) markAsProcessed(hash string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.processed[hash] = true
}

// ClearProcessed - очистка списка обработанных файлов (для тестирования)
func (w *Watcher) ClearProcessed() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.processed = make(map[string]bool)
}
