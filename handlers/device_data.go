package handlers

import (
	"TSVProcessingService/db/sqlc"
	"net/http"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
)

func (h *Handler) GetDeviceDataByUnit(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	unitGuidStr := vars["unit_guid"]

	unitGuid, err := uuid.Parse(unitGuidStr)
	if err != nil {
		h.respondWithError(w, http.StatusBadRequest, "Invalid unit_guid format")
		return
	}

	page, limit := h.getPaginationParams(r)
	offset := (page - 1) * limit

	ctx := r.Context()
	params := sqlc.ListDeviceDataByUnitParams{
		UnitGuid: unitGuid,
		Limit:    int32(limit),
		Offset:   int32(offset),
	}

	data, err := h.queries.ListDeviceDataByUnit(ctx, params)
	if err != nil {
		h.respondWithError(w, http.StatusInternalServerError, "Failed to fetch data")
		return
	}

	total, err := h.getTotalCountByUnit(ctx, unitGuid)
	if err != nil {
		h.respondWithError(w, http.StatusInternalServerError, "Failed to count total records")
		return
	}

	response := map[string]interface{}{
		"data": data,
		"pagination": Pagination{
			Page:  page,
			Limit: limit,
			Total: total,
		},
	}

	h.respondWithJSON(w, http.StatusOK, response)
}

func (h *Handler) SearchDeviceData(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query().Get("q")
	if query == "" {
		h.respondWithError(w, http.StatusBadRequest, "Search query is required")
		return
	}

	page, limit := h.getPaginationParams(r)
	offset := (page - 1) * limit

	ctx := r.Context()

	// Используем существующий запрос поиска по тексту
	// Для полнотекстового поиска лучше использовать специальные возможности PostgreSQL
	var data []sqlc.DeviceDatum
	rows, err := h.db.QueryContext(ctx,
		"SELECT * FROM device_data WHERE text ILIKE $1 ORDER BY created_at DESC LIMIT $2 OFFSET $3",
		"%"+query+"%", limit, offset)
	if err != nil {
		h.respondWithError(w, http.StatusInternalServerError, "Failed to search data")
		return
	}
	defer rows.Close()

	// Преобразуем результаты
	// ... (код обработки строк)

	h.respondWithJSON(w, http.StatusOK, data)
}
