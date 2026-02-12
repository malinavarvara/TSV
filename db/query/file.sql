-- name: CreateFile :one
INSERT INTO files (
    filename, 
    file_hash, 
    status
) VALUES (
    $1, $2, $3
) RETURNING *;

-- name: GetFileByID :one
SELECT * FROM files
WHERE id = $1 LIMIT 1;

-- name: GetFileByFilename :one
SELECT * FROM files
WHERE filename = $1 LIMIT 1;

-- name: ListFiles :many
SELECT * FROM files
ORDER BY created_at DESC
LIMIT $1
OFFSET $2;

-- name: ListFilesByStatus :many
SELECT * FROM files
WHERE status = $1
ORDER BY created_at DESC;

-- name: ListFilesByDateRange :many
SELECT * FROM files
WHERE created_at BETWEEN $1 AND $2
ORDER BY created_at DESC;

-- name: UpdateFileStatus :one
UPDATE files
SET
    status = $2,
    updated_at = CURRENT_TIMESTAMP
WHERE id = $1
RETURNING *;

-- name: UpdateFileProgress :one
UPDATE files
SET
    rows_processed = $2,
    rows_failed = $3,
    updated_at = CURRENT_TIMESTAMP
WHERE id = $1
RETURNING *;

-- name: UpdateFileWithError :one
UPDATE files
SET
    status = $2,
    error_message = $3,
    updated_at = CURRENT_TIMESTAMP
WHERE id = $1
RETURNING *;

-- name: DeleteFile :exec
DELETE FROM files
WHERE id = $1;

-- name: DeleteOldFiles :exec
DELETE FROM files
WHERE created_at < CURRENT_TIMESTAMP - interval '30 days'
AND status = $1;