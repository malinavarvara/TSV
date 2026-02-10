package handlers

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func (h *Handler) StartDirectoryMonitor() {
	// Создаем директории при старте
	if err := os.MkdirAll(h.config.InputDir, 0755); err != nil {
		log.Printf("Failed to create input directory: %v", err)
		return
	}

	if err := os.MkdirAll(h.config.OutputDir, 0755); err != nil {
		log.Printf("Failed to create output directory: %v", err)
		return
	}

	ticker := time.NewTicker(h.config.ScanInterval)
	defer ticker.Stop()

	log.Printf("Directory monitor started, scanning every %v", h.config.ScanInterval)

	for range ticker.C {
		h.scanDirectory()
	}
}

func (h *Handler) scanDirectory() {
	files, err := os.ReadDir(h.config.InputDir)
	if err != nil {
		log.Printf("Failed to read directory: %v", err)
		return
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		if !strings.HasSuffix(strings.ToLower(file.Name()), ".tsv") {
			continue
		}

		ctx := context.Background()
		_, err := h.queries.GetFileByFilename(ctx, file.Name())
		if err == nil {
			// Файл уже обработан или в процессе
			continue
		}

		filePath := filepath.Join(h.config.InputDir, file.Name())
		go h.processFile(file.Name(), filePath)

		log.Printf("Started processing file: %s", file.Name())
	}
}
