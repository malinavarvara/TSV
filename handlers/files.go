package handlers

import (
	"TSVProcessingService/db/sqlc"
	"database/sql"
	"net/http"
	"os"
	"path/filepath"

	"github.com/gorilla/mux"
)

func (h *Handler) ProcessFileHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	filename := vars["filename"]

	filePath := filepath.Join(h.config.InputDir, filename)

	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		h.respondWithError(w, http.StatusNotFound, "File not found")
		return
	}

	ctx := r.Context()
	_, err := h.queries.GetFileByFilename(ctx, filename)
	if err == nil {
		h.respondWithError(w, http.StatusConflict, "File already processed")
		return
	}

	go h.processFile(filename, filePath)

	h.respondWithJSON(w, http.StatusAccepted, SuccessResponse{
		Message: "File processing started",
	})
}

func (h *Handler) GetFiles(w http.ResponseWriter, r *http.Request) {
	page, limit := h.getPaginationParams(r)
	offset := (page - 1) * limit

	ctx := r.Context()
	params := sqlc.ListFilesParams{
		Limit:  int32(limit),
		Offset: int32(offset),
	}

	files, err := h.queries.ListFiles(ctx, params)
	if err != nil {
		h.respondWithError(w, http.StatusInternalServerError, "Failed to fetch files")
		return
	}

	total, err := h.getTotalFilesCount(ctx)
	if err != nil {
		h.respondWithError(w, http.StatusInternalServerError, "Failed to count files")
		return
	}

	response := map[string]interface{}{
		"files": files,
		"pagination": Pagination{
			Page:  page,
			Limit: limit,
			Total: total,
		},
	}

	h.respondWithJSON(w, http.StatusOK, response)
}

func (h *Handler) GetFileStatus(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	filename := vars["filename"]

	ctx := r.Context()
	file, err := h.queries.GetFileByFilename(ctx, filename)
	if err != nil {
		if err == sql.ErrNoRows {
			h.respondWithError(w, http.StatusNotFound, "File not found")
		} else {
			h.respondWithError(w, http.StatusInternalServerError, "Failed to fetch file status")
		}
		return
	}

	h.respondWithJSON(w, http.StatusOK, file)
}

func (h *Handler) GetProcessingErrors(w http.ResponseWriter, r *http.Request) {
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

	errors, err := h.queries.ListProcessingErrorsByFile(ctx, file.ID)
	if err != nil {
		h.respondWithError(w, http.StatusInternalServerError, "Failed to fetch processing errors")
		return
	}

	h.respondWithJSON(w, http.StatusOK, errors)
}

func (h *Handler) DeleteFile(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	filename := vars["filename"]

	ctx := r.Context()
	file, err := h.queries.GetFileByFilename(ctx, filename)
	if err != nil {
		h.respondWithError(w, http.StatusNotFound, "File not found")
		return
	}

	// Удаляем связанные данные
	if err := h.queries.DeleteDeviceDataByFileID(ctx, file.ID); err != nil {
		h.respondWithError(w, http.StatusInternalServerError, "Failed to delete device data")
		return
	}

	if err := h.queries.DeleteProcessingErrorsByFile(ctx, file.ID); err != nil {
		h.respondWithError(w, http.StatusInternalServerError, "Failed to delete processing errors")
		return
	}

	// Удаляем запись о файле
	if err := h.queries.DeleteFile(ctx, file.ID); err != nil {
		h.respondWithError(w, http.StatusInternalServerError, "Failed to delete file record")
		return
	}

	h.respondWithJSON(w, http.StatusOK, SuccessResponse{
		Message: "File and related data deleted successfully",
	})
}
