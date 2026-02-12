// internal/processor/processor.go
package processor

import (
	"TSVProcessingService/db/sqlc"
	"TSVProcessingService/internal/config"
	"TSVProcessingService/internal/watcher"
	"bufio"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jung-kurt/gofpdf/v2"
)

// Processor обрабатывает TSV файлы
type Processor struct {
	db      *sql.DB
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

// ProcessingError представляет ошибку обработки строки
type ProcessingError struct {
	LineNumber   sql.NullInt32
	RawLine      sql.NullString
	ErrorMessage string
	FieldName    sql.NullString
}

// NewProcessor создает новый процессор
func NewProcessor(db *sql.DB, queries *sqlc.Queries, config *config.DirectoryConfig) *Processor {
	return &Processor{
		db:      db,
		queries: queries,
		config:  config,
	}
}

// ---------------------------------------------------------------------
// Основной метод обработки файла
// ---------------------------------------------------------------------

// ProcessFile – основной метод обработки одного TSV файла
func (p *Processor) ProcessFile(ctx context.Context, fileInfo watcher.FileInfo) error {
	log.Printf("[Processor] 🔄 Processing file: %s", fileInfo.Name)

	// 1. СНАЧАЛА проверяем, не был ли этот файл уже обработан
	existingFile, err := p.queries.GetFileByFilename(ctx, fileInfo.Name)
	if err == nil {
		log.Printf("[Processor] File %s already processed (status: %s)", fileInfo.Name, existingFile.Status.String)
		p.moveExistingFile(fileInfo.Path, existingFile.Status.String)
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("failed to check existing file: %w", err)
	}

	// 2. ТОЛЬКО ТЕПЕРЬ проверяем, готов ли файл к чтению
	if err := p.waitForFileReady(fileInfo.Path, 10*time.Second); err != nil {
		return fmt.Errorf("file not ready: %w", err)
	}

	// 3. Транзакционная обработка файла
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	qtx := p.queries.WithTx(tx)

	// 4. Создание записи о файле
	fileParams := sqlc.CreateFileParams{
		Filename: fileInfo.Name,
		FileHash: fileInfo.Hash,
		Status:   sql.NullString{String: "processing", Valid: true},
	}
	file, err := qtx.CreateFile(ctx, fileParams)
	if err != nil {
		return fmt.Errorf("failed to create file record: %w", err)
	}
	log.Printf("[Processor] Created file record ID: %d", file.ID)

	// 5. Парсинг TSV (новая реализация)
	rows, parseErrors := p.parseTSVFile(fileInfo.Path, file.ID)

	// 6. Сохранение ошибок парсинга
	for _, perr := range parseErrors {
		errParams := sqlc.CreateProcessingErrorParams{
			FileID:       file.ID,
			LineNumber:   perr.LineNumber,
			RawLine:      perr.RawLine,
			ErrorMessage: perr.ErrorMessage,
			FieldName:    perr.FieldName,
		}
		if _, err := qtx.CreateProcessingError(ctx, errParams); err != nil {
			log.Printf("[Processor] Failed to save processing error: %v", err)
		}
	}

	// 7. Сохранение валидных строк в device_data
	successCount := int32(0)
	failedCount := int32(0)

	for _, row := range rows {
		params := sqlc.CreateDeviceDataParams{
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
		if _, err := qtx.CreateDeviceData(ctx, params); err != nil {
			log.Printf("[Processor] ❌ Error inserting device data: %v", err)
			failedCount++
		} else {
			successCount++
		}
	}

	// 8. Обновление статистики файла
	updateParams := sqlc.UpdateFileProgressParams{
		ID:            file.ID,
		RowsProcessed: sql.NullInt32{Int32: successCount, Valid: true},
		RowsFailed:    sql.NullInt32{Int32: failedCount, Valid: true},
	}
	if _, err := qtx.UpdateFileProgress(ctx, updateParams); err != nil {
		log.Printf("[Processor] Failed to update file progress: %v", err)
	}

	// 9. Определение финального статуса
	status := "completed"
	if successCount == 0 {
		status = "failed"
	} else if failedCount > 0 {
		status = "partial"
	}
	statusParams := sqlc.UpdateFileStatusParams{
		ID:     file.ID,
		Status: sql.NullString{String: status, Valid: true},
	}
	if _, err := qtx.UpdateFileStatus(ctx, statusParams); err != nil {
		log.Printf("[Processor] Failed to update file status: %v", err)
	}

	// 10. Фиксация транзакции
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	log.Printf("[Processor] ✅ Transaction committed for file %s", fileInfo.Name)

	// 11. Генерация PDF‑отчётов для каждого unit_guid (вне транзакции)
	if err := p.generateReports(ctx, file.ID, rows); err != nil {
		log.Printf("[Processor] Error generating reports: %v", err)
	}

	// 12. Перемещение файла в архив или папку ошибок
	if status == "completed" || status == "partial" {
		if err := p.moveFile(fileInfo.Path, p.config.ArchivePath, fileInfo.Name); err != nil {
			log.Printf("[Processor] Failed to archive file %s: %v", fileInfo.Name, err)
		} else {
			log.Printf("[Processor] 📦 File moved to archive: %s", fileInfo.Name)
		}
	} else {
		if err := p.moveFile(fileInfo.Path, p.config.ErrorPath, fileInfo.Name); err != nil {
			log.Printf("[Processor] Failed to move failed file %s: %v", fileInfo.Name, err)
		} else {
			log.Printf("[Processor] ⚠️ File moved to error folder: %s", fileInfo.Name)
		}
	}

	log.Printf("[Processor] ✅ Finished processing %s (success: %d, failed: %d)",
		fileInfo.Name, successCount, failedCount)
	return nil
}

// ---------------------------------------------------------------------
// Новая реализация парсинга TSV (без encoding/csv)
// ---------------------------------------------------------------------

// parseTSVFile открывает файл и построчно разбирает его.
// Разделитель – строго символ табуляции ('\t').
func (p *Processor) parseTSVFile(filePath string, fileID int64) ([]TSVRow, []ProcessingError) {
	log.Printf("[Processor] 🔍 Parsing TSV (simple split): %s", filePath)

	f, err := os.Open(filePath)
	if err != nil {
		return nil, []ProcessingError{{
			ErrorMessage: fmt.Sprintf("failed to open file: %v", err),
		}}
	}
	defer f.Close()

	var rows []TSVRow
	var errors []ProcessingError
	lineNumber := int32(0)
	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		line := scanner.Text()
		lineNumber++

		// Пропускаем пустые строки
		if strings.TrimSpace(line) == "" {
			continue
		}

		// Пропускаем комментарии (начинаются с #)
		if strings.HasPrefix(strings.TrimSpace(line), "#") {
			continue
		}

		// Разбиваем по табуляции
		fields := strings.Split(line, "\t")

		// Пропускаем строку заголовка (первое поле не является числом)
		if len(fields) > 0 {
			if _, err := strconv.Atoi(strings.TrimSpace(fields[0])); err != nil {
				log.Printf("[Processor] Skipping header line: %s", line)
				continue
			}
		}

		// Минимальное количество полей: n, mqtt, invid, unit_guid
		if len(fields) < 4 {
			errors = append(errors, ProcessingError{
				LineNumber:   sql.NullInt32{Int32: lineNumber, Valid: true},
				RawLine:      sql.NullString{String: line, Valid: true},
				ErrorMessage: fmt.Sprintf("insufficient fields: got %d, need at least 4", len(fields)),
			})
			continue
		}

		// Парсинг полей
		row, parseErr := p.parseLine(fields, lineNumber)
		if parseErr != nil {
			errors = append(errors, ProcessingError{
				LineNumber:   sql.NullInt32{Int32: lineNumber, Valid: true},
				RawLine:      sql.NullString{String: line, Valid: true},
				ErrorMessage: parseErr.Error(),
			})
			continue
		}
		rows = append(rows, row)
	}

	if err := scanner.Err(); err != nil {
		errors = append(errors, ProcessingError{
			ErrorMessage: fmt.Sprintf("scanner error: %v", err),
		})
	}

	log.Printf("[Processor] 📊 Parsed %d rows, %d errors", len(rows), len(errors))
	return rows, errors
}

// parseLine преобразует массив полей в TSVRow.
// Индексы колонок (начиная с 0):
//   0: n
//   1: mqtt (всегда пусто)
//   2: invid
//   3: unit_guid
//   4: msg_id
//   5: text
//   6: context
//   7: class
//   8: level
//   9: area
//  10: addr
//  11: block
//  12: type
//  13: bit
//  14: invert_bit
func (p *Processor) parseLine(fields []string, lineNumber int32) (TSVRow, error) {
	row := TSVRow{LineNumber: lineNumber}

	// UUID на позиции 3 – строго обязателен
	guidStr := strings.TrimSpace(fields[3])
	guid, err := uuid.Parse(guidStr)
	if err != nil {
		return row, fmt.Errorf("invalid unit_guid at column 4: %w", err)
	}
	row.UnitGuid = guid

	// invid (индекс 2)
	if val := strings.TrimSpace(fields[2]); val != "" {
		row.Invid = sql.NullString{String: val, Valid: true}
	}

	// msg_id (индекс 4)
	if len(fields) > 4 {
		if val := strings.TrimSpace(fields[4]); val != "" {
			row.MsgID = sql.NullString{String: val, Valid: true}
		}
	}

	// text (индекс 5)
	if len(fields) > 5 {
		if val := strings.TrimSpace(fields[5]); val != "" {
			row.Text = sql.NullString{String: val, Valid: true}
		}
	}

	// context (индекс 6) – игнорируем (всегда NULL)

	// class (индекс 7)
	if len(fields) > 7 {
		if val := strings.TrimSpace(fields[7]); val != "" {
			if isValidClass(val) {
				row.Class = sql.NullString{String: val, Valid: true}
			} else {
				return row, fmt.Errorf("invalid class value: %s", val)
			}
		}
	}

	// level (индекс 8)
	if len(fields) > 8 {
		val := strings.TrimSpace(fields[8])
		if val != "" {
			level, err := strconv.ParseInt(val, 10, 32)
			if err != nil {
				return row, fmt.Errorf("invalid level (not integer): %s", val)
			}
			row.Level = sql.NullInt32{Int32: int32(level), Valid: true}
		}
	}

	// area (индекс 9)
	if len(fields) > 9 {
		if val := strings.TrimSpace(fields[9]); val != "" {
			row.Area = sql.NullString{String: val, Valid: true}
		}
	}

	// addr (индекс 10)
	if len(fields) > 10 {
		if val := strings.TrimSpace(fields[10]); val != "" {
			row.Addr = sql.NullString{String: val, Valid: true}
		}
	}

	// block (индекс 11)
	if len(fields) > 11 {
		if val := strings.TrimSpace(fields[11]); val != "" {
			row.Block = sql.NullString{String: val, Valid: true}
		}
	}

	// type (индекс 12)
	if len(fields) > 12 {
		if val := strings.TrimSpace(fields[12]); val != "" {
			row.Type = sql.NullString{String: val, Valid: true}
		}
	}

	// bit (индекс 13)
	if len(fields) > 13 {
		val := strings.TrimSpace(fields[13])
		if val != "" {
			bit, err := strconv.ParseInt(val, 10, 32)
			if err != nil {
				return row, fmt.Errorf("invalid bit (not integer): %s", val)
			}
			row.Bit = sql.NullInt32{Int32: int32(bit), Valid: true}
		}
	}

	// invert_bit (индекс 14)
	if len(fields) > 14 {
		val := strings.TrimSpace(fields[14])
		if val != "" {
			invert, err := parseInvertBit(val)
			if err != nil {
				return row, fmt.Errorf("invalid invert_bit: %w", err)
			}
			row.InvertBit = sql.NullBool{Bool: invert, Valid: true}
		}
	}

	return row, nil
}

// isValidClass проверяет допустимые значения class
func isValidClass(class string) bool {
	allowed := map[string]bool{
		"alarm":   true,
		"warning": true,
		"info":    true,
		"event":   true,
		"comand":  true,
		"waiting": true,
		"working": true,
	}
	return allowed[strings.ToLower(class)]
}

// parseInvertBit преобразует строку в bool
func parseInvertBit(field string) (bool, error) {
	field = strings.ToLower(strings.TrimSpace(field))
	switch field {
	case "true", "1", "yes":
		return true, nil
	case "false", "0", "no", "":
		return false, nil
	default:
		if val, err := strconv.ParseBool(field); err == nil {
			return val, nil
		}
		return false, fmt.Errorf("cannot parse invert_bit: %s", field)
	}
}

// ---------------------------------------------------------------------
// Генерация PDF‑отчётов
// ---------------------------------------------------------------------

// generateReports группирует данные по unit_guid и создаёт отдельный PDF‑отчёт
func (p *Processor) generateReports(ctx context.Context, fileID int64, rows []TSVRow) error {
	byUnit := make(map[uuid.UUID][]TSVRow)
	for _, row := range rows {
		byUnit[row.UnitGuid] = append(byUnit[row.UnitGuid], row)
	}

	for guid, data := range byUnit {
		reportPath, err := p.createPDFReport(guid, data)
		if err != nil {
			log.Printf("[Processor] ❌ Failed to create PDF for %s: %v", guid, err)
			continue
		}

		params := sqlc.CreateReportParams{
			UnitGuid:   guid,
			ReportType: sql.NullString{String: "pdf", Valid: true},
			FilePath:   reportPath,
		}
		if _, err := p.queries.CreateReport(ctx, params); err != nil {
			log.Printf("[Processor] ❌ Failed to save report record: %v", err)
		} else {
			log.Printf("[Processor] ✅ PDF report created: %s", reportPath)
		}
	}
	return nil
}

// createPDFReport генерирует PDF‑файл с данными устройства
func (p *Processor) createPDFReport(unitGuid uuid.UUID, data []TSVRow) (string, error) {
	if err := os.MkdirAll(p.config.OutputPath, 0755); err != nil {
		return "", err
	}

	timestamp := time.Now().Format("20060102_150405")
	filename := fmt.Sprintf("%s_%s.pdf", unitGuid.String(), timestamp)
	path := filepath.Join(p.config.OutputPath, filename)

	pdf := gofpdf.New("P", "mm", "A4", "")
	pdf.AddPage()
	pdf.SetFont("Arial", "B", 16)
	pdf.Cell(40, 10, "Device Report")
	pdf.Ln(12)

	pdf.SetFont("Arial", "", 12)
	pdf.Cell(40, 10, "Unit GUID: "+unitGuid.String())
	pdf.Ln(6)
	pdf.Cell(40, 10, "Generated: "+time.Now().Format(time.RFC3339))
	pdf.Ln(6)
	pdf.Cell(40, 10, fmt.Sprintf("Total records: %d", len(data)))
	pdf.Ln(10)

	pdf.SetFont("Arial", "B", 11)
	pdf.Cell(40, 8, "Device Data:")
	pdf.Ln(8)
	pdf.SetFont("Arial", "", 10)

	for i, row := range data {
		pdf.Cell(40, 6, fmt.Sprintf("Record %d:", i+1))
		pdf.Ln(5)
		if row.Invid.Valid {
			pdf.Cell(40, 5, "  Inventory ID: "+row.Invid.String)
			pdf.Ln(5)
		}
		if row.MsgID.Valid {
			pdf.Cell(40, 5, "  Message ID: "+row.MsgID.String)
			pdf.Ln(5)
		}
		if row.Text.Valid {
			pdf.Cell(40, 5, "  Text: "+row.Text.String)
			pdf.Ln(5)
		}
		if row.Class.Valid {
			pdf.Cell(40, 5, "  Class: "+row.Class.String)
			pdf.Ln(5)
		}
		if row.Level.Valid {
			pdf.Cell(40, 5, fmt.Sprintf("  Level: %d", row.Level.Int32))
			pdf.Ln(5)
		}
		if row.Area.Valid {
			pdf.Cell(40, 5, "  Area: "+row.Area.String)
			pdf.Ln(5)
		}
		if row.Addr.Valid {
			pdf.Cell(40, 5, "  Address: "+row.Addr.String)
			pdf.Ln(5)
		}
		if row.Block.Valid {
			pdf.Cell(40, 5, "  Block: "+row.Block.String)
			pdf.Ln(5)
		}
		if row.Type.Valid {
			pdf.Cell(40, 5, "  Type: "+row.Type.String)
			pdf.Ln(5)
		}
		if row.Bit.Valid {
			pdf.Cell(40, 5, fmt.Sprintf("  Bit: %d", row.Bit.Int32))
			pdf.Ln(5)
		}
		if row.InvertBit.Valid {
			pdf.Cell(40, 5, fmt.Sprintf("  Invert Bit: %v", row.InvertBit.Bool))
			pdf.Ln(5)
		}
		pdf.Ln(4)
	}

	if err := pdf.OutputFileAndClose(path); err != nil {
		return "", fmt.Errorf("failed to save PDF: %w", err)
	}
	return path, nil
}

// GenerateReportForUnit генерирует отчёт для конкретного устройства по всем данным в БД
func (p *Processor) GenerateReportForUnit(ctx context.Context, unitGuid uuid.UUID) error {
	log.Printf("[Processor] 📊 Generating PDF report for unit: %s", unitGuid)

	// Получаем все данные устройства (используем пагинацию с большим лимитом)
	deviceData, err := p.queries.ListDeviceDataByUnit(ctx, sqlc.ListDeviceDataByUnitParams{
		UnitGuid: unitGuid,
		Limit:    10000,
		Offset:   0,
	})
	if err != nil {
		return fmt.Errorf("failed to fetch device data: %w", err)
	}
	if len(deviceData) == 0 {
		return fmt.Errorf("no data found for unit %s", unitGuid)
	}

	rows := make([]TSVRow, 0, len(deviceData))
	for _, d := range deviceData {
		rows = append(rows, TSVRow{
			UnitGuid:   d.UnitGuid,
			Mqtt:       d.Mqtt,
			Invid:      d.Invid,
			MsgID:      d.MsgID,
			Text:       d.Text,
			Context:    d.Context,
			Class:      d.Class,
			Level:      d.Level,
			Area:       d.Area,
			Addr:       d.Addr,
			Block:      d.Block,
			Type:       d.Type,
			Bit:        d.Bit,
			InvertBit:  d.InvertBit,
			LineNumber: d.LineNumber,
		})
	}

	reportPath, err := p.createPDFReport(unitGuid, rows)
	if err != nil {
		return fmt.Errorf("failed to create PDF report: %w", err)
	}

	params := sqlc.CreateReportParams{
		UnitGuid:   unitGuid,
		ReportType: sql.NullString{String: "pdf", Valid: true},
		FilePath:   reportPath,
	}
	if _, err := p.queries.CreateReport(ctx, params); err != nil {
		log.Printf("[Processor] ⚠️ Report generated but DB record failed: %v", err)
	} else {
		log.Printf("[Processor] ✅ PDF report saved: %s", reportPath)
	}
	return nil
}

// ---------------------------------------------------------------------
// Работа с файловой системой
// ---------------------------------------------------------------------

// waitForFileReady проверяет, что файл доступен для чтения и его размер стабилен.
func (p *Processor) waitForFileReady(filePath string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var prevSize int64 = -1
	for time.Now().Before(deadline) {
		info, err := os.Stat(filePath)
		if err != nil {
			return err
		}
		if info.Size() > 0 && info.Size() == prevSize {
			return nil
		}
		prevSize = info.Size()
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("file size not stable within %v", timeout)
}

// moveFile перемещает или копирует файл в целевую директорию.
// Если rename не работает (cross-device), выполняет copy+remove.
func (p *Processor) moveFile(src, destDir, filename string) error {
	if err := os.MkdirAll(destDir, 0755); err != nil {
		return err
	}
	dest := filepath.Join(destDir, filename)

	// Пробуем rename
	err := os.Rename(src, dest)
	if err == nil {
		return nil
	}
	// Если ошибка cross-device, копируем и удаляем
	if strings.Contains(err.Error(), "cross-device") {
		if err := p.copyFile(src, dest); err != nil {
			return fmt.Errorf("copy failed: %w", err)
		}
		if err := os.Remove(src); err != nil {
			return fmt.Errorf("original file remove failed: %w", err)
		}
		return nil
	}
	return err
}

// copyFile копирует содержимое файла.
func (p *Processor) copyFile(src, dst string) error {
	source, err := os.Open(src)
	if err != nil {
		return err
	}
	defer source.Close()

	destination, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer destination.Close()

	_, err = io.Copy(destination, source)
	return err
}

// moveExistingFile перемещает уже обработанный файл в соответствующую папку.
func (p *Processor) moveExistingFile(filePath, status string) {
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		log.Printf("[Processor] File %s already moved or deleted, skipping", filePath)
		return
	}

	switch status {
	case "completed", "partial":
		if err := p.moveFile(filePath, p.config.ArchivePath, filepath.Base(filePath)); err != nil {
			log.Printf("[Processor] Failed to archive already processed file: %v", err)
		}
	case "failed":
		if err := p.moveFile(filePath, p.config.ErrorPath, filepath.Base(filePath)); err != nil {
			log.Printf("[Processor] Failed to move failed file: %v", err)
		}
	default:
		// Статус "processing" – ничего не делаем
	}
}
