package watcher

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestWatcher(t *testing.T) (*Watcher, string, func()) {
	tmpDir, err := os.MkdirTemp("", "watcher_test_*")
	require.NoError(t, err)

	watchDir := filepath.Join(tmpDir, "watch")
	err = os.Mkdir(watchDir, 0755)
	require.NoError(t, err)

	w := NewWatcher(watchDir, 100*time.Millisecond, 10)
	cleanup := func() {
		w.Stop()
		os.RemoveAll(tmpDir)
	}
	return w, watchDir, cleanup
}

func createTestFile(t *testing.T, dir, name, content string) string {
	path := filepath.Join(dir, name)
	err := os.WriteFile(path, []byte(content), 0644)
	require.NoError(t, err)
	return path
}

// ---------------------------------------------------------------------
// Тесты scanDirectory и processFile
// ---------------------------------------------------------------------

func TestScanDirectory_FindsTSVFiles(t *testing.T) {
	w, watchDir, cleanup := setupTestWatcher(t)
	defer cleanup()

	createTestFile(t, watchDir, "test1.tsv", "a\tb\tc")
	createTestFile(t, watchDir, "test2.txt", "hello")
	err := os.Mkdir(filepath.Join(watchDir, "subdir"), 0755)
	require.NoError(t, err)

	w.scanDirectory()

	select {
	case fileInfo := <-w.fileQueue:
		assert.Equal(t, "test1.tsv", fileInfo.Name)
	default:
		t.Fatal("Expected file in queue")
	}

	select {
	case <-w.fileQueue:
		t.Fatal("Unexpected file in queue")
	default:
	}
}

func TestProcessFile_QueueWithTimeout(t *testing.T) {
	w, watchDir, cleanup := setupTestWatcher(t)
	defer cleanup()

	path := createTestFile(t, watchDir, "test.tsv", "content")
	w.processFile(path)

	select {
	case fileInfo := <-w.fileQueue:
		assert.Equal(t, "test.tsv", fileInfo.Name)
		assert.Equal(t, int64(len("content")), fileInfo.Size)
		assert.NotEmpty(t, fileInfo.Hash)
	default:
		t.Fatal("File not queued")
	}
}

func TestProcessFile_FileNotFound(t *testing.T) {
	w, _, cleanup := setupTestWatcher(t)
	defer cleanup()

	w.processFile("/does/not/exist.tsv")
	select {
	case <-w.fileQueue:
		t.Fatal("Should not queue non-existent file")
	default:
	}
}

// ---------------------------------------------------------------------
// Тест calculateFileHash
// ---------------------------------------------------------------------

func TestCalculateFileHash(t *testing.T) {
	w, watchDir, cleanup := setupTestWatcher(t)
	defer cleanup()

	content := "test content"
	path := createTestFile(t, watchDir, "hash.tsv", content)

	hash, err := w.calculateFileHash(path)
	require.NoError(t, err)
	assert.NotEmpty(t, hash)

	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	h := sha256.New()
	_, err = io.Copy(h, f)
	require.NoError(t, err)
	expected := hex.EncodeToString(h.Sum(nil))
	assert.Equal(t, expected, hash)
}

// ---------------------------------------------------------------------
// Тест SendToQueue
// ---------------------------------------------------------------------

func TestSendToQueue_BlocksUntilTimeout(t *testing.T) {
	w := NewWatcher("/tmp", time.Second, 1)
	defer w.Stop()

	fileInfo := FileInfo{Name: "file1.tsv"}
	err := w.SendToQueue(fileInfo)
	require.NoError(t, err)

	fileInfo2 := FileInfo{Name: "file2.tsv"}
	err = w.SendToQueue(fileInfo2)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "queue is full")
}

func TestSendToQueue_Success(t *testing.T) {
	w, _, cleanup := setupTestWatcher(t)
	defer cleanup()

	fileInfo := FileInfo{Name: "test.tsv", Path: "/some/path"}
	err := w.SendToQueue(fileInfo)
	require.NoError(t, err)

	select {
	case received := <-w.fileQueue:
		assert.Equal(t, "test.tsv", received.Name)
	default:
		t.Fatal("Queue empty")
	}
}

// ---------------------------------------------------------------------
// Тест GetFileQueue
// ---------------------------------------------------------------------

func TestGetFileQueue_Works(t *testing.T) {
	w, _, cleanup := setupTestWatcher(t)
	defer cleanup()

	ch := w.GetFileQueue()
	assert.NotNil(t, ch)

	fileInfo := FileInfo{Name: "test.tsv"}
	go func() {
		w.fileQueue <- fileInfo
	}()

	select {
	case received := <-ch:
		assert.Equal(t, "test.tsv", received.Name)
	case <-time.After(time.Second):
		t.Fatal("Channel not working")
	}
}

// ---------------------------------------------------------------------
// Тест Stop
// ---------------------------------------------------------------------

func TestStop_ClosesQueue(t *testing.T) {
	w, _, cleanup := setupTestWatcher(t)
	defer cleanup()

	w.Stop()
	_, ok := <-w.fileQueue
	assert.False(t, ok, "channel should be closed")
}

// ---------------------------------------------------------------------
// Интеграционный тест Start/Stop (исправлен – ожидание 600 мс)
// ---------------------------------------------------------------------

func TestWatcher_StartStop(t *testing.T) {
	w, watchDir, cleanup := setupTestWatcher(t)
	w.interval = 500 * time.Millisecond
	defer cleanup()

	go w.Start()
	time.Sleep(100 * time.Millisecond) // даём время на первый scan

	createTestFile(t, watchDir, "new.tsv", "data")

	// Ждём дольше, чем интервал сканирования, чтобы файл точно был обнаружен
	time.Sleep(600 * time.Millisecond)

	select {
	case fileInfo := <-w.fileQueue:
		assert.Equal(t, "new.tsv", fileInfo.Name)
	default:
		t.Fatal("File not queued")
	}

	// Проверяем, что нет дублей
	select {
	case <-w.fileQueue:
		t.Fatal("Duplicate file in queue")
	case <-time.After(100 * time.Millisecond):
		// OK
	}

	w.Stop()
	_, ok := <-w.fileQueue
	assert.False(t, ok)
}
