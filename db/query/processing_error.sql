-- name: CreateProcessingError :one
INSERT INTO processing_errors (
    file_id,
    line_number,
    raw_line,
    error_message,
    field_name
) VALUES (
    $1, $2, $3, $4, $5
) RETURNING *;

-- name: GetProcessingErrorByID :one
SELECT * FROM processing_errors
WHERE id = $1 LIMIT 1;

-- name: ListProcessingErrorsByFile :many
SELECT * FROM processing_errors
WHERE file_id = $1
ORDER BY line_number;

-- name: ListProcessingErrorsSummary :many
SELECT
    error_message,
    field_name,
    COUNT(*) as error_count,
    MIN(line_number) as first_line,
    MAX(line_number) as last_line
FROM processing_errors
WHERE file_id = $1
GROUP BY error_message, field_name
ORDER BY error_count DESC;

-- name: UpdateProcessingError :one
UPDATE processing_errors
SET
    error_message = $2,
    field_name = $3
WHERE id = $1
RETURNING *;

-- name: DeleteProcessingError :exec
DELETE FROM processing_errors
WHERE id = $1;

-- name: DeleteProcessingErrorsByFile :exec
DELETE FROM processing_errors
WHERE file_id = $1;