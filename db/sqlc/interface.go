//go:generate mockgen -source=interface.go -destination=../../internal/mocks/mock_queries.go -package=mocks
package sqlc

import (
	"context"
	"database/sql"

	"github.com/google/uuid"
)

type Querier interface {
	// ------------------------------------------------------------
	// api_log.sql
	// ------------------------------------------------------------
	CleanupOldApiLogs(ctx context.Context) error
	CreateApiLog(ctx context.Context, arg CreateApiLogParams) (ApiLog, error)
	DeleteApiLog(ctx context.Context, id int64) error
	GetApiLogByID(ctx context.Context, id int64) (ApiLog, error)
	GetApiStatistics(ctx context.Context) ([]GetApiStatisticsRow, error)
	ListApiErrors(ctx context.Context, statusCode sql.NullInt32) ([]ApiLog, error)
	ListApiLogs(ctx context.Context, arg ListApiLogsParams) ([]ApiLog, error)
	ListApiLogsByEndpoint(ctx context.Context, endpoint string) ([]ApiLog, error)
	ListApiLogsByUnit(ctx context.Context, unitGuid uuid.NullUUID) ([]ApiLog, error)
	ListSlowRequests(ctx context.Context, responseTimeMs sql.NullInt32) ([]ApiLog, error)
	UpdateApiLog(ctx context.Context, arg UpdateApiLogParams) (ApiLog, error)

	// ------------------------------------------------------------
	// device_data.sql
	// ------------------------------------------------------------
	BulkInsertDeviceData(ctx context.Context, arg BulkInsertDeviceDataParams) error
	CreateDeviceData(ctx context.Context, arg CreateDeviceDataParams) (DeviceDatum, error)
	DeleteDeviceData(ctx context.Context, id int64) error
	DeleteDeviceDataByFileID(ctx context.Context, fileID int64) error
	GetDeviceDataByFileID(ctx context.Context, fileID int64) ([]DeviceDatum, error)
	GetDeviceDataByID(ctx context.Context, id int64) (DeviceDatum, error)
	GetDeviceStatistics(ctx context.Context, fileID int64) (GetDeviceStatisticsRow, error)
	ListDeviceDataByClass(ctx context.Context, arg ListDeviceDataByClassParams) ([]DeviceDatum, error)
	ListDeviceDataByUnit(ctx context.Context, arg ListDeviceDataByUnitParams) ([]DeviceDatum, error)
	SearchDeviceDataText(ctx context.Context, arg SearchDeviceDataTextParams) ([]DeviceDatum, error)
	UpdateDeviceData(ctx context.Context, arg UpdateDeviceDataParams) (DeviceDatum, error)

	// ------------------------------------------------------------
	// file.sql
	// ------------------------------------------------------------
	CreateFile(ctx context.Context, arg CreateFileParams) (File, error)
	DeleteFile(ctx context.Context, id int64) error
	DeleteOldFiles(ctx context.Context, status sql.NullString) error
	GetFileByFilename(ctx context.Context, filename string) (File, error)
	GetFileByID(ctx context.Context, id int64) (File, error)
	ListFiles(ctx context.Context, arg ListFilesParams) ([]File, error)
	ListFilesByDateRange(ctx context.Context, arg ListFilesByDateRangeParams) ([]File, error)
	ListFilesByStatus(ctx context.Context, status sql.NullString) ([]File, error)
	UpdateFileProgress(ctx context.Context, arg UpdateFileProgressParams) (File, error)
	UpdateFileStatus(ctx context.Context, arg UpdateFileStatusParams) (File, error)
	UpdateFileWithError(ctx context.Context, arg UpdateFileWithErrorParams) (File, error)

	// ------------------------------------------------------------
	// processing_error.sql
	// ------------------------------------------------------------
	CreateProcessingError(ctx context.Context, arg CreateProcessingErrorParams) (ProcessingError, error)
	DeleteProcessingError(ctx context.Context, id int64) error
	DeleteProcessingErrorsByFile(ctx context.Context, fileID int64) error
	GetProcessingErrorByID(ctx context.Context, id int64) (ProcessingError, error)
	ListProcessingErrorsByFile(ctx context.Context, fileID int64) ([]ProcessingError, error)
	ListProcessingErrorsSummary(ctx context.Context, fileID int64) ([]ListProcessingErrorsSummaryRow, error)
	UpdateProcessingError(ctx context.Context, arg UpdateProcessingErrorParams) (ProcessingError, error)

	// ------------------------------------------------------------
	// report.sql
	// ------------------------------------------------------------
	CreateReport(ctx context.Context, arg CreateReportParams) (Report, error)
	DeleteOldReports(ctx context.Context) error
	DeleteReport(ctx context.Context, id int64) error
	GetReportByID(ctx context.Context, id int64) (Report, error)
	GetReportsByDateRange(ctx context.Context, arg GetReportsByDateRangeParams) ([]Report, error)
	GetReportsByUnit(ctx context.Context, unitGuid uuid.UUID) ([]Report, error)
	ListRecentReports(ctx context.Context, arg ListRecentReportsParams) ([]Report, error)
	UpdateReportPath(ctx context.Context, arg UpdateReportPathParams) (Report, error)
}
