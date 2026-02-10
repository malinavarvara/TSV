package handlers

import (
	"TSVProcessingService/db/sqlc"
	"context"
	"database/sql"
	"net/http"
	"time"

	"github.com/google/uuid"
)

type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

func (h *Handler) LoggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rw := &responseWriter{w, http.StatusOK}

		next.ServeHTTP(rw, r)

		duration := time.Since(start)

		// Извлекаем unit_guid из URL если есть
		var unitGuidStr string
		// Здесь можно добавить логику извлечения из path

		var unitGuid uuid.NullUUID
		if unitGuidStr != "" {
			if guid, err := uuid.Parse(unitGuidStr); err == nil {
				unitGuid = uuid.NullUUID{UUID: guid, Valid: true}
			}
		}

		// Асинхронно логируем запрос
		go func() {
			ctx := context.Background()
			h.queries.CreateApiLog(ctx, sqlc.CreateApiLogParams{
				Endpoint:       r.URL.Path,
				UnitGuid:       unitGuid,
				ResponseTimeMs: sql.NullInt32{Int32: int32(duration.Milliseconds()), Valid: true},
				StatusCode:     sql.NullInt32{Int32: int32(rw.statusCode), Valid: true},
			})
		}()
	})
}

func (h *Handler) RecoveryMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				h.respondWithError(w, http.StatusInternalServerError, "Internal server error")
			}
		}()

		next.ServeHTTP(w, r)
	})
}

func (h *Handler) CORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}
