package handlers

import (
	"TSVProcessingService/db/sqlc"
	"context"
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/google/uuid"
)

func (h *Handler) processFile(filename, filePath string) {
	ctx := context.Background()

	fileHash := h.calculateFileHash(filePath)
	fileRecord, err := h.queries.CreateFile(ctx, sqlc.CreateFileParams{
		Filename: filename,
		FileHash: fileHash,
		Status:   sql.NullString{String: "processing", Valid: true},
	})
	if err != nil {
		fmt.Printf("Failed to create file record: %v\n", err)
		return
	}

	file, err := os.Open(filePath)
	if err != nil {
		h.updateFileWithError(ctx, fileRecord.ID, "Failed to open file", err)
		return
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = '\t'
	reader.LazyQuotes = true

	if _, err := reader.Read(); err != nil {
		h.updateFileWithError(ctx, fileRecord.ID, "Failed to read header", err)
		return
	}

	var rowsProcessed int32 = 0
	var rowsFailed int32 = 0
	var deviceData []DeviceData
	lineNumber := 1

	for {
		lineNumber++
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			rowsFailed++
			h.createProcessingError(ctx, fileRecord.ID, lineNumber, strings.Join(record, "\t"), "Failed to parse line", "")
			continue
		}

		data, parseErr := h.parseTSVRecord(record)
		if parseErr != nil {
			rowsFailed++
			h.createProcessingError(ctx, fileRecord.ID, lineNumber, strings.Join(record, "\t"), parseErr.Error(), "")
			continue
		}

		deviceData = append(deviceData, data)
		rowsProcessed++
	}

	if err := h.saveDeviceData(ctx, fileRecord.ID, deviceData); err != nil {
		h.updateFileWithError(ctx, fileRecord.ID, "Failed to save device data", err)
		return
	}

	h.queries.UpdateFileProgress(ctx, sqlc.UpdateFileProgressParams{
		ID:            fileRecord.ID,
		RowsProcessed: sql.NullInt32{Int32: rowsProcessed, Valid: true},
		RowsFailed:    sql.NullInt32{Int32: rowsFailed, Valid: true},
	})

	h.generateReports(ctx, fileRecord.ID, deviceData)

	h.queries.UpdateFileStatus(ctx, sqlc.UpdateFileStatusParams{
		ID:     fileRecord.ID,
		Status: sql.NullString{String: "completed", Valid: true},
	})
}

func (h *Handler) parseTSVRecord(record []string) (DeviceData, error) {
	if len(record) < 14 {
		return DeviceData{}, errors.New("invalid number of columns")
	}

	level, _ := strconv.Atoi(record[8])
	bit, _ := strconv.Atoi(record[13])
	invertBit := record[14] == "1" || strings.ToLower(record[14]) == "true"

	return DeviceData{
		Mqtt:      record[1],
		Invid:     record[2],
		UnitGuid:  record[3],
		MsgID:     record[4],
		Text:      record[5],
		Context:   record[6],
		Class:     record[7],
		Level:     level,
		Area:      record[9],
		Addr:      record[10],
		Block:     record[11],
		Type:      record[12],
		Bit:       bit,
		InvertBit: invertBit,
	}, nil
}

func (h *Handler) saveDeviceData(ctx context.Context, fileID int64, data []DeviceData) error {
	for _, item := range data {
		unitGuid, err := uuid.Parse(item.UnitGuid)
		if err != nil {
			continue
		}

		_, err = h.queries.CreateDeviceData(ctx, sqlc.CreateDeviceDataParams{
			FileID:     fileID,
			UnitGuid:   unitGuid,
			Mqtt:       sql.NullString{String: item.Mqtt, Valid: item.Mqtt != ""},
			Invid:      sql.NullString{String: item.Invid, Valid: item.Invid != ""},
			MsgID:      sql.NullString{String: item.MsgID, Valid: item.MsgID != ""},
			Text:       sql.NullString{String: item.Text, Valid: item.Text != ""},
			Context:    sql.NullString{String: item.Context, Valid: item.Context != ""},
			Class:      sql.NullString{String: item.Class, Valid: item.Class != ""},
			Level:      sql.NullInt32{Int32: int32(item.Level), Valid: true},
			Area:       sql.NullString{String: item.Area, Valid: item.Area != ""},
			Addr:       sql.NullString{String: item.Addr, Valid: item.Addr != ""},
			Block:      sql.NullString{String: item.Block, Valid: item.Block != ""},
			Type:       sql.NullString{String: item.Type, Valid: item.Type != ""},
			Bit:        sql.NullInt32{Int32: int32(item.Bit), Valid: true},
			InvertBit:  sql.NullBool{Bool: item.InvertBit, Valid: true},
			LineNumber: 0,
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (h *Handler) calculateFileHash(filePath string) string {
	info, err := os.Stat(filePath)
	if err != nil {
		return ""
	}
	return fmt.Sprintf("%d_%d", info.Size(), info.ModTime().Unix())
}

func (h *Handler) updateFileWithError(ctx context.Context, fileID int64, message string, err error) {
	h.queries.UpdateFileWithError(ctx, sqlc.UpdateFileWithErrorParams{
		ID:           fileID,
		Status:       sql.NullString{String: "failed", Valid: true},
		ErrorMessage: sql.NullString{String: fmt.Sprintf("%s: %v", message, err), Valid: true},
	})
}

func (h *Handler) createProcessingError(ctx context.Context, fileID int64, lineNumber int, rawLine, errorMessage, fieldName string) {
	h.queries.CreateProcessingError(ctx, sqlc.CreateProcessingErrorParams{
		FileID:       fileID,
		LineNumber:   sql.NullInt32{Int32: int32(lineNumber), Valid: true},
		RawLine:      sql.NullString{String: rawLine, Valid: rawLine != ""},
		ErrorMessage: errorMessage,
		FieldName:    sql.NullString{String: fieldName, Valid: fieldName != ""},
	})
}
