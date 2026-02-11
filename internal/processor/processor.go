package processor

import (
	"TSVProcessingService/db/sqlc"
	"TSVProcessingService/internal/config"
	"TSVProcessingService/internal/watcher"
	"bytes"
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
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

// normalizeTSV заменяет два и более пробела на табуляцию
func normalizeTSV(content []byte) []byte {
	re := regexp.MustCompile(`[ ]{2,}`)
	return re.ReplaceAll(content, []byte("\t"))
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

func (p *Processor) parseTSVFile(filePath string, fileID int64) ([]TSVRow, []ProcessingError) {
	log.Printf("🔍 Начинаем парсинг файла: %s", filePath)

	// 1. Читаем весь файл
	content, err := os.ReadFile(filePath)
	if err != nil {
		log.Printf("❌ Ошибка чтения файла: %v", err)
		return nil, []ProcessingError{{
			ErrorMessage: fmt.Sprintf("failed to read file: %v", err),
		}}
	}

	// 2. Нормализуем: два+ пробела -> табуляция
	normalized := normalizeTSV(content)

	// 3. Создаём CSV Reader с разделителем TAB
	reader := csv.NewReader(bytes.NewReader(normalized))
	reader.Comma = '\t'
	reader.FieldsPerRecord = -1    // разрешаем разное количество полей
	reader.TrimLeadingSpace = true // обрезаем пробелы в начале/конце

	var rows []TSVRow
	var errors []ProcessingError

	lineNumber := int32(0)
	headerSkipped := false

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			lineNumber++
			log.Printf("❌ Ошибка чтения строки %d: %v", lineNumber, err)
			errors = append(errors, ProcessingError{
				LineNumber:   sql.NullInt32{Int32: lineNumber, Valid: true},
				ErrorMessage: fmt.Sprintf("CSV read error: %v", err),
			})
			continue
		}

		lineNumber++
		rawLine := strings.Join(record, "\t") // для логирования

		// Пропускаем пустые строки
		if len(record) == 0 || (len(record) == 1 && strings.TrimSpace(record[0]) == "") {
			continue
		}

		// Пропускаем комментарии (строки, начинающиеся с #)
		if len(record) > 0 && strings.HasPrefix(strings.TrimSpace(record[0]), "#") {
			continue
		}

		// Первая не-комментарийная строка — заголовок
		if !headerSkipped {
			headerSkipped = true
			log.Printf("Пропускаем заголовок: %s", rawLine)
			continue
		}

		// Парсим строку данных
		row, err := p.parseLine(record, lineNumber, rawLine)
		if err != nil {
			log.Printf("❌ Ошибка строки %d: %v", lineNumber, err)
			errors = append(errors, ProcessingError{
				LineNumber:   sql.NullInt32{Int32: lineNumber, Valid: true},
				RawLine:      sql.NullString{String: rawLine, Valid: true},
				ErrorMessage: err.Error(),
			})
			continue
		}

		rows = append(rows, row)
		log.Printf("✅ Строка %d: unit_guid=%s, msg_id=%v", lineNumber, row.UnitGuid, row.MsgID)
	}

	log.Printf("📊 Парсинг завершен: %d строк, %d ошибок", len(rows), len(errors))
	return rows, errors
}

// parseLine - улучшенная версия
// parseLine находит UUID и распределяет поля относительно его позиции
func (p *Processor) parseLine(fields []string, lineNumber int32, rawLine string) (TSVRow, error) {
	row := TSVRow{LineNumber: lineNumber}

	// 1. Ищем поле с корректным UUID (unit_guid)
	guidIndex := -1
	var guid uuid.UUID
	var err error
	for i, field := range fields {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}
		guid, err = uuid.Parse(field)
		if err == nil {
			guidIndex = i
			break
		}
	}
	if guidIndex == -1 {
		return row, fmt.Errorf("unit_guid (UUID) not found in line")
	}
	row.UnitGuid = guid

	// 2. Поле перед GUID — invid (инвентарный номер)
	if guidIndex-1 >= 0 {
		if val := strings.TrimSpace(fields[guidIndex-1]); val != "" {
			row.Invid = sql.NullString{String: val, Valid: true}
		}
	}

	// 3. Ещё раньше — mqtt (если есть)
	if guidIndex-2 >= 0 {
		if val := strings.TrimSpace(fields[guidIndex-2]); val != "" && val != row.Invid.String {
			// Защита от ошибочного захвата номера строки
			row.Mqtt = sql.NullString{String: val, Valid: true}
		}
	}

	// 4. Поля после GUID — строго по порядку
	if guidIndex+1 < len(fields) {
		if val := strings.TrimSpace(fields[guidIndex+1]); val != "" {
			row.MsgID = sql.NullString{String: val, Valid: true}
		}
	}
	if guidIndex+2 < len(fields) {
		if val := strings.TrimSpace(fields[guidIndex+2]); val != "" {
			row.Text = sql.NullString{String: val, Valid: true}
		}
	}
	if guidIndex+3 < len(fields) {
		if val := strings.TrimSpace(fields[guidIndex+3]); val != "" {
			row.Context = sql.NullString{String: val, Valid: true}
		}
	}
	if guidIndex+4 < len(fields) {
		if val := strings.TrimSpace(fields[guidIndex+4]); val != "" {
			row.Class = sql.NullString{String: val, Valid: true}
		}
	}
	if guidIndex+5 < len(fields) {
		if val := strings.TrimSpace(fields[guidIndex+5]); val != "" {
			if level, err := parseLevel(val); err == nil {
				row.Level = sql.NullInt32{Int32: level, Valid: true}
			}
		}
	}
	if guidIndex+6 < len(fields) {
		if val := strings.TrimSpace(fields[guidIndex+6]); val != "" {
			row.Area = sql.NullString{String: val, Valid: true}
		}
	}
	if guidIndex+7 < len(fields) {
		if val := strings.TrimSpace(fields[guidIndex+7]); val != "" {
			row.Addr = sql.NullString{String: val, Valid: true}
		}
	}
	if guidIndex+8 < len(fields) {
		if val := strings.TrimSpace(fields[guidIndex+8]); val != "" {
			row.Block = sql.NullString{String: val, Valid: true}
		}
	}
	if guidIndex+9 < len(fields) {
		if val := strings.TrimSpace(fields[guidIndex+9]); val != "" {
			row.Type = sql.NullString{String: val, Valid: true}
		}
	}
	if guidIndex+10 < len(fields) {
		if val := strings.TrimSpace(fields[guidIndex+10]); val != "" {
			if bit, err := parseBit(val); err == nil {
				row.Bit = sql.NullInt32{Int32: bit, Valid: true}
			}
		}
	}
	if guidIndex+11 < len(fields) {
		if val := strings.TrimSpace(fields[guidIndex+11]); val != "" {
			if invert, err := parseInvertBit(val); err == nil {
				row.InvertBit = sql.NullBool{Bool: invert, Valid: true}
			}
		}
	}

	return row, nil
}

// Вспомогательные функции для парсинга
func parseLevel(field string) (int32, error) {
	level, err := strconv.ParseInt(field, 10, 32)
	if err != nil {
		return 0, err
	}
	return int32(level), nil
}

func parseBit(field string) (int32, error) {
	bit, err := strconv.ParseInt(field, 10, 32)
	if err != nil {
		return 0, err
	}
	return int32(bit), nil
}

func parseInvertBit(field string) (bool, error) {
	// Может быть "true"/"false", "1"/"0", или "да"/"нет"
	field = strings.ToLower(strings.TrimSpace(field))
	switch field {
	case "true", "1", "да", "yes":
		return true, nil
	case "false", "0", "нет", "no", "":
		return false, nil
	default:
		// Пробуем как число
		if val, err := strconv.ParseBool(field); err == nil {
			return val, nil
		}
		return false, fmt.Errorf("cannot parse invert_bit: %s", field)
	}
}

// generateReports генерирует отчеты для данных
func (p *Processor) generateReports(ctx context.Context, fileID int64, rows []TSVRow) error {
	// Группируем по unit_guid
	byUnit := make(map[uuid.UUID][]TSVRow)
	for _, row := range rows {
		byUnit[row.UnitGuid] = append(byUnit[row.UnitGuid], row)
	}

	for guid, data := range byUnit {
		reportPath, err := p.createReport(guid, data)
		if err != nil {
			log.Printf("❌ Ошибка создания отчёта для %s: %v", guid, err)
			continue
		}

		params := sqlc.CreateReportParams{
			UnitGuid:   guid,
			ReportType: sql.NullString{String: "txt", Valid: true},
			FilePath:   reportPath,
		}
		if _, err := p.queries.CreateReport(ctx, params); err != nil {
			log.Printf("❌ Ошибка сохранения отчёта в БД: %v", err)
		} else {
			log.Printf("✅ Отчёт создан: %s", reportPath)
		}
	}
	return nil
}

func (p *Processor) createReport(unitGuid uuid.UUID, data []TSVRow) (string, error) {
	if err := os.MkdirAll(p.config.OutputPath, 0755); err != nil {
		return "", err
	}

	timestamp := time.Now().Format("20060102_150405")
	filename := fmt.Sprintf("%s_%s.txt", unitGuid.String(), timestamp)
	path := filepath.Join(p.config.OutputPath, filename)

	content := p.generateTextReport(unitGuid, data)
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		return "", err
	}
	return path, nil
}

func (p *Processor) generateTextReport(unitGuid uuid.UUID, data []TSVRow) string {
	var b strings.Builder
	b.WriteString("Device Report\n")
	b.WriteString("=============\n\n")
	b.WriteString("Unit GUID: " + unitGuid.String() + "\n")
	b.WriteString("Generated: " + time.Now().Format(time.RFC3339) + "\n")
	b.WriteString("Total records: " + fmt.Sprintf("%d", len(data)) + "\n\n")

	b.WriteString("Device Data:\n")
	b.WriteString("------------\n")
	for i, row := range data {
		b.WriteString(fmt.Sprintf("\nRecord %d:\n", i+1))
		if row.MsgID.Valid {
			b.WriteString("  Message ID: " + row.MsgID.String + "\n")
		}
		if row.Text.Valid {
			b.WriteString("  Text: " + row.Text.String + "\n")
		}
		if row.Class.Valid {
			b.WriteString("  Class: " + row.Class.String + "\n")
		}
		if row.Level.Valid {
			b.WriteString("  Level: " + fmt.Sprintf("%d", row.Level.Int32) + "\n")
		}
		if row.Addr.Valid {
			b.WriteString("  Address: " + row.Addr.String + "\n")
		}
	}
	return b.String()
}
