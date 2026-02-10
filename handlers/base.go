package handlers

import (
	"TSVProcessingService/db/sqlc"
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"
)

type Handler struct {
	queries *sqlc.Queries
	db      *sql.DB
	config  *Config
}

type Config struct {
	InputDir     string
	OutputDir    string
	ScanInterval time.Duration
}

type ErrorResponse struct {
	Error string `json:"error"`
}

type SuccessResponse struct {
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

type Pagination struct {
	Page  int `json:"page"`
	Limit int `json:"limit"`
	Total int `json:"total"`
}

type DeviceData struct {
	Mqtt      string `json:"mqtt"`
	Invid     string `json:"invid"`
	UnitGuid  string `json:"unit_guid"`
	MsgID     string `json:"msg_id"`
	Text      string `json:"text"`
	Context   string `json:"context"`
	Class     string `json:"class"`
	Level     int    `json:"level"`
	Area      string `json:"area"`
	Addr      string `json:"addr"`
	Block     string `json:"block"`
	Type      string `json:"type"`
	Bit       int    `json:"bit"`
	InvertBit bool   `json:"invert_bit"`
}

func NewHandler(db *sql.DB, queries *sqlc.Queries, config *Config) *Handler {
	return &Handler{
		db:      db,
		queries: queries,
		config:  config,
	}
}

func (h *Handler) getPaginationParams(r *http.Request) (page, limit int) {
	page = 1
	limit = 50

	if p := r.URL.Query().Get("page"); p != "" {
		if pInt, err := strconv.Atoi(p); err == nil && pInt > 0 {
			page = pInt
		}
	}

	if l := r.URL.Query().Get("limit"); l != "" {
		if lInt, err := strconv.Atoi(l); err == nil && lInt > 0 && lInt <= 1000 {
			limit = lInt
		}
	}

	return page, limit
}

func (h *Handler) respondWithError(w http.ResponseWriter, code int, message string) {
	h.respondWithJSON(w, code, ErrorResponse{Error: message})
}

func (h *Handler) respondWithJSON(w http.ResponseWriter, code int, payload interface{}) {
	response, err := json.Marshal(payload)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	w.Write(response)
}

func (h *Handler) getTotalCountByUnit(ctx context.Context, unitGuid uuid.UUID) (int, error) {
	var count int
	err := h.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM device_data WHERE unit_guid = $1",
		unitGuid).Scan(&count)
	return count, err
}

func (h *Handler) getTotalFilesCount(ctx context.Context) (int, error) {
	var count int
	err := h.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM files").Scan(&count)
	return count, err
}
