package handlers

import (
	"TSVProcessingService/db/sqlc"
	"database/sql"
	"net/http"
	"os"
	"path/filepath"
	"strconv"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
)

func (h *Handler) GetReportsByUnit(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	unitGuidStr := vars["unit_guid"]

	unitGuid, err := uuid.Parse(unitGuidStr)
	if err != nil {
		h.respondWithError(w, http.StatusBadRequest, "Invalid unit_guid format")
		return
	}

	ctx := r.Context()
	reports, err := h.queries.GetReportsByUnit(ctx, unitGuid)
	if err != nil {
		h.respondWithError(w, http.StatusInternalServerError, "Failed to fetch reports")
		return
	}

	h.respondWithJSON(w, http.StatusOK, reports)
}

func (h *Handler) DownloadReport(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	reportID := vars["id"]

	id, err := strconv.ParseInt(reportID, 10, 64)
	if err != nil {
		h.respondWithError(w, http.StatusBadRequest, "Invalid report ID")
		return
	}

	ctx := r.Context()
	report, err := h.queries.GetReportByID(ctx, id)
	if err != nil {
		if err == sql.ErrNoRows {
			h.respondWithError(w, http.StatusNotFound, "Report not found")
		} else {
			h.respondWithError(w, http.StatusInternalServerError, "Failed to fetch report")
		}
		return
	}

	// Проверяем существование файла
	if _, err := os.Stat(report.FilePath); os.IsNotExist(err) {
		h.respondWithError(w, http.StatusNotFound, "Report file not found")
		return
	}

	// Отправляем файл
	filename := filepath.Base(report.FilePath)
	w.Header().Set("Content-Disposition", "attachment; filename="+filename)
	w.Header().Set("Content-Type", "application/pdf")
	http.ServeFile(w, r, report.FilePath)
}

func (h *Handler) GenerateReport(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	unitGuidStr := vars["unit_guid"]

	unitGuid, err := uuid.Parse(unitGuidStr)
	if err != nil {
		h.respondWithError(w, http.StatusBadRequest, "Invalid unit_guid format")
		return
	}

	// Получаем данные устройства
	ctx := r.Context()
	page, limit := h.getPaginationParams(r)
	offset := (page - 1) * limit

	params := sqlc.ListDeviceDataByUnitParams{
		UnitGuid: unitGuid,
		Limit:    int32(limit),
		Offset:   int32(offset),
	}

	data, err := h.queries.ListDeviceDataByUnit(ctx, params)
	if err != nil {
		h.respondWithError(w, http.StatusInternalServerError, "Failed to fetch device data")
		return
	}

	// Конвертируем в структуру DeviceData для генерации отчета
	var deviceData []DeviceData
	for _, d := range data {
		deviceData = append(deviceData, DeviceData{
			Mqtt:      d.Mqtt.String,
			Invid:     d.Invid.String,
			UnitGuid:  d.UnitGuid.String(),
			MsgID:     d.MsgID.String,
			Text:      d.Text.String,
			Context:   d.Context.String,
			Class:     d.Class.String,
			Level:     int(d.Level.Int32),
			Area:      d.Area.String,
			Addr:      d.Addr.String,
			Block:     d.Block.String,
			Type:      d.Type.String,
			Bit:       int(d.Bit.Int32),
			InvertBit: d.InvertBit.Bool,
		})
	}

	// Генерируем отчет
	reportPath, err := h.createPDFReport(unitGuid, deviceData)
	if err != nil {
		h.respondWithError(w, http.StatusInternalServerError, "Failed to generate report")
		return
	}

	// Сохраняем информацию об отчете
	report, err := h.queries.CreateReport(ctx, sqlc.CreateReportParams{
		UnitGuid:   unitGuid,
		ReportType: sql.NullString{String: "pdf", Valid: true},
		FilePath:   reportPath,
	})
	if err != nil {
		h.respondWithError(w, http.StatusInternalServerError, "Failed to save report record")
		return
	}

	h.respondWithJSON(w, http.StatusCreated, SuccessResponse{
		Message: "Report generated successfully",
		Data:    report,
	})
}
