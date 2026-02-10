package processor

import (
	"TSVProcessingService/db/sqlc"
	"TSVProcessingService/internal/config"
	"TSVProcessingService/internal/watcher"
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Processor обрабатывает TSV файлы
type Processor struct {
	queries *sqlc.Queries
	config  *config.DirectoryConfig
}

// TSVRow представляет строку из TSV файла
type TSVRow struct {
	UnitGuid   uuid.UUID
	Mqtt       sql.NullString
	Invid      sql.NullString
	MsgID      sql.NullString
	Text       sql.NullString
	Context    sql.NullString
	Class      sql.NullString
	Level      sql.NullInt32
	Area       sql.NullString
	Addr       sql.NullString
	Block      sql.NullString
	Type       sql.NullString
	Bit        sql.NullInt32
	InvertBit  sql.NullBool
	LineNumber int32
}

// ProcessingError представляет ошибку обработки
type ProcessingError struct {
	LineNumber   sql.NullInt32
	RawLine      sql.NullString
	ErrorMessage string
	FieldName    sql.NullString
}

// NewProcessor создает новый процессор
func NewProcessor(queries *sqlc.Queries, config *config.DirectoryConfig) *Processor {
	return &Processor{
		queries: queries,
		config:  config,
	}
}

// ProcessFile обрабатывает TSV файл
func (p *Processor) ProcessFile(ctx context.Context, fileInfo watcher.FileInfo) error {
	log.Printf("🔄 Processing file: %s", fileInfo.Name)

	// 1. Проверяем, не обрабатывался ли уже файл
	existingFile, err := p.queries.GetFileByFilename(ctx, fileInfo.Name)
	if err == nil && existingFile.FileHash == fileInfo.Hash {
		log.Printf("File already processed: %s", fileInfo.Name)
		return nil
	}

	// 2. Создаем запись о файле
	fileParams := sqlc.CreateFileParams{
		Filename: fileInfo.Name,
		FileHash: fileInfo.Hash,
		Status:   sql.NullString{String: "processing", Valid: true},
	}

	file, err := p.queries.CreateFile(ctx, fileParams)
	if err != nil {
		return fmt.Errorf("failed to create file record: %w", err)
	}

	log.Printf("Created file record with ID: %d", file.ID)

	// 3. Парсим TSV файл
	rows, errors := p.parseTSVFile(fileInfo.Path, file.ID)
	if len(errors) > 0 {
		// Сохраняем ошибки обработки
		for _, processingErr := range errors {
			errParams := sqlc.CreateProcessingErrorParams{
				FileID:       file.ID,
				LineNumber:   processingErr.LineNumber,
				RawLine:      processingErr.RawLine,
				ErrorMessage: processingErr.ErrorMessage,
				FieldName:    processingErr.FieldName,
			}
			if _, err := p.queries.CreateProcessingError(ctx, errParams); err != nil {
				log.Printf("Error saving processing error: %v", err)
			}
		}
	}

	// 4. Сохраняем данные в базу
	successCount := int32(0)
	failedCount := int32(0)

	for _, row := range rows {
		deviceDataParams := sqlc.CreateDeviceDataParams{
			FileID:     file.ID,
			UnitGuid:   row.UnitGuid,
			Mqtt:       row.Mqtt,
			Invid:      row.Invid,
			MsgID:      row.MsgID,
			Text:       row.Text,
			Context:    row.Context,
			Class:      row.Class,
			Level:      row.Level,
			Area:       row.Area,
			Addr:       row.Addr,
			Block:      row.Block,
			Type:       row.Type,
			Bit:        row.Bit,
			InvertBit:  row.InvertBit,
			LineNumber: row.LineNumber,
		}

		_, err := p.queries.CreateDeviceData(ctx, deviceDataParams)
		if err != nil {
			log.Printf("Error saving device data: %v", err)
			failedCount++
			continue
		}
		successCount++
	}

	// 5. Обновляем статус файла
	updateParams := sqlc.UpdateFileProgressParams{
		ID:            file.ID,
		RowsProcessed: sql.NullInt32{Int32: successCount, Valid: true},
		RowsFailed:    sql.NullInt32{Int32: failedCount, Valid: true},
	}

	if _, err := p.queries.UpdateFileProgress(ctx, updateParams); err != nil {
		log.Printf("Error updating file progress: %v", err)
	}

	// 6. Устанавливаем финальный статус
	status := "completed"
	if failedCount > 0 && failedCount == int32(len(rows)) {
		status = "failed"
	} else if failedCount > 0 {
		status = "partial"
	}

	statusParams := sqlc.UpdateFileStatusParams{
		ID:     file.ID,
		Status: sql.NullString{String: status, Valid: true},
	}

	if _, err := p.queries.UpdateFileStatus(ctx, statusParams); err != nil {
		log.Printf("Error updating file status: %v", err)
	}

	// 7. Генерируем отчеты
	if err := p.generateReports(ctx, file.ID, rows); err != nil {
		log.Printf("Error generating reports: %v", err)
	}

	log.Printf("✅ Finished processing: %s. Success: %d, Failed: %d",
		fileInfo.Name, successCount, failedCount)

	return nil
}

// parseTSVFile парсит TSV файл
func (p *Processor) parseTSVFile(filePath string, fileID int64) ([]TSVRow, []ProcessingError) {
	// TODO: Реализовать парсинг TSV файла
	// Временная заглушка
	return []TSVRow{}, []ProcessingError{}
}

// generateReports генерирует отчеты для данных
func (p *Processor) generateReports(ctx context.Context, fileID int64, rows []TSVRow) error {
	// Группируем данные по unit_guid
	deviceDataByUnit := make(map[uuid.UUID][]sqlc.DeviceDatum)

	// TODO: Преобразовать TSVRow в sqlc.DeviceDatum и сгруппировать

	for unitGuid, data := range deviceDataByUnit {
		// Генерируем отчет для каждого устройства
		reportPath, err := p.createReport(unitGuid, data)
		if err != nil {
			log.Printf("Failed to create report for %s: %v", unitGuid, err)
			continue
		}

		// Сохраняем информацию об отчете
		reportParams := sqlc.CreateReportParams{
			UnitGuid:   unitGuid,
			ReportType: sql.NullString{String: "pdf", Valid: true},
			FilePath:   reportPath,
		}

		if _, err := p.queries.CreateReport(ctx, reportParams); err != nil {
			log.Printf("Failed to save report record: %v", err)
		}
	}

	return nil
}

// createReport создает отчет для устройства
func (p *Processor) createReport(unitGuid uuid.UUID, data []sqlc.DeviceDatum) (string, error) {
	// Создаем выходную директорию если не существует
	if err := os.MkdirAll(p.config.OutputPath, 0755); err != nil {
		return "", err
	}

	// Генерируем имя файла
	timestamp := time.Now().Format("20060102_150405")
	filename := unitGuid.String() + "_" + timestamp + ".txt"
	path := filepath.Join(p.config.OutputPath, filename)

	// TODO: Заменить на реальную генерацию PDF/RTF/DOC
	// Сейчас создаем простой текстовый файл
	content := p.generateTextReport(unitGuid, data)

	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		return "", err
	}

	return path, nil
}

// generateTextReport генерирует текстовый отчет
func (p *Processor) generateTextReport(unitGuid uuid.UUID, data []sqlc.DeviceDatum) string {
	var builder strings.Builder

	builder.WriteString("Device Report\n")
	builder.WriteString("=============\n\n")
	builder.WriteString("Unit GUID: " + unitGuid.String() + "\n")
	builder.WriteString("Generated: " + time.Now().Format(time.RFC3339) + "\n")
	builder.WriteString("Total records: " + fmt.Sprintf("%d", len(data)) + "\n\n")

	builder.WriteString("Device Data:\n")
	builder.WriteString("------------\n")

	for i, item := range data {
		builder.WriteString(fmt.Sprintf("\nRecord %d:\n", i+1))
		if item.MsgID.Valid {
			builder.WriteString("  Message ID: " + item.MsgID.String + "\n")
		}
		if item.Text.Valid {
			builder.WriteString("  Text: " + item.Text.String + "\n")
		}
		if item.Class.Valid {
			builder.WriteString("  Class: " + item.Class.String + "\n")
		}
		if item.Level.Valid {
			builder.WriteString("  Level: " + fmt.Sprintf("%d", item.Level.Int32) + "\n")
		}
	}

	return builder.String()
}
