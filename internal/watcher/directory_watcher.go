// internal/watcher/directory_watcher.go
package watcher

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// FileInfo представляет информацию о файле, который будет обработан.
type FileInfo struct {
	Path    string    // полный путь к файлу
	Name    string    // имя файла
	Size    int64     // размер в байтах
	ModTime time.Time // время последней модификации
	Hash    string    // SHA256 хеш содержимого файла
}

// Watcher отвечает за периодическое сканирование директории,
// обнаружение новых .tsv файлов и передачу их в очередь на обработку.
type Watcher struct {
	watchDir  string        // директория для наблюдения
	interval  time.Duration // интервал сканирования
	fileQueue chan FileInfo // буферизированный канал с файлами для обработки
	stopChan  chan struct{} // сигнал остановки
	closed    bool          // флаг для защиты от повторного закрытия каналов
	mu        sync.Mutex    // мьютекс для атомарного закрытия
}

// NewWatcher создаёт новый экземпляр Watcher.
// watchDir   – путь к директории для мониторинга.
// interval   – периодичность сканирования.
// queueSize  – размер буфера очереди файлов.
func NewWatcher(watchDir string, interval time.Duration, queueSize int) *Watcher {
	return &Watcher{
		watchDir:  watchDir,
		interval:  interval,
		fileQueue: make(chan FileInfo, queueSize),
		stopChan:  make(chan struct{}),
	}
}

// Start запускает цикл сканирования директории.
// Запускается в отдельной горутине; работает до вызова Stop().
func (w *Watcher) Start() {
	log.Printf("[Watcher] Starting directory watcher for: %s (interval: %v)", w.watchDir, w.interval)

	// Первоначальное сканирование
	w.scanDirectory()

	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			w.scanDirectory()
		case <-w.stopChan:
			log.Println("[Watcher] Directory watcher stopped")
			return
		}
	}
}

// Stop останавливает Watcher и закрывает канал fileQueue.
// Может быть вызвана многократно безопасно.
func (w *Watcher) Stop() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return
	}
	close(w.stopChan)
	close(w.fileQueue)
	w.closed = true
	log.Println("[Watcher] File queue closed")
}

// GetFileQueue возвращает канал для чтения FileInfo.
// Используется воркерами для получения файлов.
func (w *Watcher) GetFileQueue() <-chan FileInfo {
	return w.fileQueue
}

// SendToQueue позволяет внешним компонентам (например, API) вручную
// поставить файл в очередь обработки. Блокируется до освобождения места
// в канале, но не дольше timeout (5 секунд).
func (w *Watcher) SendToQueue(fileInfo FileInfo) error {
	select {
	case w.fileQueue <- fileInfo:
		log.Printf("[Watcher] Manually queued file: %s", fileInfo.Name)
		return nil
	case <-time.After(5 * time.Second):
		return fmt.Errorf("queue is full, timeout after 5s")
	}
}

// scanDirectory читает содержимое watchDir, отбирает .tsv файлы
// и для каждого вызывает processFile.
func (w *Watcher) scanDirectory() {
	entries, err := os.ReadDir(w.watchDir)
	if err != nil {
		log.Printf("[Watcher] Error reading directory %s: %v", w.watchDir, err)
		return
	}

	for _, entry := range entries {
		// Пропускаем поддиректории и скрытые файлы
		if entry.IsDir() || strings.HasPrefix(entry.Name(), ".") {
			continue
		}
		// Интересуют только файлы с расширением .tsv
		if !strings.HasSuffix(strings.ToLower(entry.Name()), ".tsv") {
			continue
		}

		filePath := filepath.Join(w.watchDir, entry.Name())
		w.processFile(filePath)
	}
}

// processFile собирает информацию о файле, вычисляет хеш и
// отправляет его в очередь (с таймаутом).
func (w *Watcher) processFile(filePath string) {
	info, err := os.Stat(filePath)
	if err != nil {
		log.Printf("[Watcher] Error stating file %s: %v", filePath, err)
		return
	}

	// Вычисляем SHA256 хеш содержимого файла
	hash, err := w.calculateFileHash(filePath)
	if err != nil {
		log.Printf("[Watcher] Error calculating hash for %s: %v", filePath, err)
		return
	}

	fileInfo := FileInfo{
		Path:    filePath,
		Name:    info.Name(),
		Size:    info.Size(),
		ModTime: info.ModTime(),
		Hash:    hash,
	}

	// Отправляем в очередь с таймаутом 5 секунд.
	// Если очередь заполнена, ждём; если таймаут истёк – логируем ошибку.
	select {
	case w.fileQueue <- fileInfo:
		log.Printf("[Watcher] Queued file: %s (size: %d bytes, hash: %s)",
			fileInfo.Name, fileInfo.Size, fileInfo.Hash[:8])
	case <-time.After(5 * time.Second):
		log.Printf("[Watcher] Queue is full, cannot queue file: %s", fileInfo.Name)
	}
}

// calculateFileHash вычисляет SHA256 хеш содержимого файла.
func (w *Watcher) calculateFileHash(filePath string) (string, error) {
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
