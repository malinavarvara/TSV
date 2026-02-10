package handlers

import (
	"database/sql"
	"net/http"
	"time"

	"github.com/gorilla/mux"
)

func (h *Handler) GetStatistics(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	stats, err := h.queries.GetApiStatistics(ctx)
	if err != nil {
		h.respondWithError(w, http.StatusInternalServerError, "Failed to fetch statistics")
		return
	}

	h.respondWithJSON(w, http.StatusOK, stats)
}

func (h *Handler) GetDeviceStatistics(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	filename := vars["filename"]

	ctx := r.Context()
	file, err := h.queries.GetFileByFilename(ctx, filename)
	if err != nil {
		if err == sql.ErrNoRows {
			h.respondWithError(w, http.StatusNotFound, "File not found")
		} else {
			h.respondWithError(w, http.StatusInternalServerError, "Failed to fetch file")
		}
		return
	}

	stats, err := h.queries.GetDeviceStatistics(ctx, file.ID)
	if err != nil {
		h.respondWithError(w, http.StatusInternalServerError, "Failed to fetch device statistics")
		return
	}

	h.respondWithJSON(w, http.StatusOK, stats)
}

func (h *Handler) GetProcessingStatistics(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	filename := vars["filename"]

	ctx := r.Context()
	file, err := h.queries.GetFileByFilename(ctx, filename)
	if err != nil {
		if err == sql.ErrNoRows {
			h.respondWithError(w, http.StatusNotFound, "File not found")
		} else {
			h.respondWithError(w, http.StatusInternalServerError, "Failed to fetch file")
		}
		return
	}

	// Получаем сводную статистику ошибок
	errorsSummary, err := h.queries.ListProcessingErrorsSummary(ctx, file.ID)
	if err != nil {
		h.respondWithError(w, http.StatusInternalServerError, "Failed to fetch errors summary")
		return
	}

	// Получаем общую статистику файла
	stats := map[string]interface{}{
		"filename":       file.Filename,
		"status":         file.Status.String,
		"rows_processed": file.RowsProcessed.Int32,
		"rows_failed":    file.RowsFailed.Int32,
		"created_at":     file.CreatedAt.Time.Format(time.RFC3339),
		"errors_summary": errorsSummary,
	}

	h.respondWithJSON(w, http.StatusOK, stats)
}
