package handlers

import (
	"TSVProcessingService/db/sqlc"
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/google/uuid"
)

func (h *Handler) generateReports(ctx context.Context, fileID int64, data []DeviceData) {
	devicesByUnit := make(map[string][]DeviceData)
	for _, item := range data {
		devicesByUnit[item.UnitGuid] = append(devicesByUnit[item.UnitGuid], item)
	}

	for unitGuid, deviceData := range devicesByUnit {
		guid, err := uuid.Parse(unitGuid)
		if err != nil {
			continue
		}

		reportPath, err := h.createPDFReport(guid, deviceData)
		if err != nil {
			// Логируем ошибку, но не прерываем обработку других устройств
			continue
		}

		_, err = h.queries.CreateReport(ctx, sqlc.CreateReportParams{
			UnitGuid:   guid,
			ReportType: sql.NullString{String: "pdf", Valid: true},
			FilePath:   reportPath,
		})
		if err != nil {
			// Логируем ошибку
		}
	}
}

func (h *Handler) createPDFReport(unitGuid uuid.UUID, data []DeviceData) (string, error) {
	// Создаем выходную директорию если не существует
	if err := os.MkdirAll(h.config.OutputDir, 0755); err != nil {
		return "", err
	}

	// Исправлено: использование конкатенации вместо fmt.Sprintf
	filename := unitGuid.String() + "_" + time.Now().Format("20060102_150405") + ".pdf"
	path := filepath.Join(h.config.OutputDir, filename)

	// Временная реализация - создаем текстовый файл
	// В реальном проекте используйте библиотеку для генерации PDF (например, gofpdf)
	content := "Device Report\n"
	content += "Unit GUID: " + unitGuid.String() + "\n"
	content += "Generated: " + time.Now().Format(time.RFC3339) + "\n\n"
	content += "Total records: " + strconv.Itoa(len(data)) + "\n\n"

	for i, item := range data {
		content += "Record " + strconv.Itoa(i+1) + ":\n"
		content += "  Message ID: " + item.MsgID + "\n"
		content += "  Text: " + item.Text + "\n"
		content += "  Class: " + item.Class + "\n"
		content += "  Level: " + strconv.Itoa(item.Level) + "\n"
		content += "  Context: " + item.Context + "\n"
		content += "  Area: " + item.Area + "\n"
		content += "  Address: " + item.Addr + "\n"
		content += "  Type: " + item.Type + "\n"
		content += "  Bit: " + strconv.Itoa(item.Bit) + "\n"
		content += "  Invert Bit: " + strconv.FormatBool(item.InvertBit) + "\n\n"
	}

	err := os.WriteFile(path, []byte(content), 0644)
	if err != nil {
		return "", err
	}

	return path, nil
}
