-- name: CreateReport :one
INSERT INTO reports (
    unit_guid,
    report_type,
    file_path
) VALUES (
    $1, $2, $3
) RETURNING *;

-- name: GetReportByID :one
SELECT * FROM reports
WHERE id = $1 LIMIT 1;

-- name: GetReportsByUnit :many
SELECT * FROM reports
WHERE unit_guid = $1
ORDER BY generated_at DESC;

-- name: ListRecentReports :many
SELECT * FROM reports
ORDER BY generated_at DESC
LIMIT $1
OFFSET $2;

-- name: GetReportsByDateRange :many
SELECT * FROM reports
WHERE generated_at BETWEEN $1 AND $2
ORDER BY generated_at DESC;

-- name: UpdateReportPath :one
UPDATE reports
SET
    file_path = $2
WHERE id = $1
RETURNING *;

-- name: DeleteReport :exec
DELETE FROM reports
WHERE id = $1;

-- name: DeleteOldReports :exec
DELETE FROM reports
WHERE generated_at < now() - interval '365 days';