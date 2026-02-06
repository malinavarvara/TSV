-- name: CreateApiLog :one
INSERT INTO api_logs (
    endpoint,
    unit_guid,
    response_time_ms,
    status_code
) VALUES (
    $1, $2, $3, $4
) RETURNING *;

-- name: GetApiLogByID :one
SELECT * FROM api_logs
WHERE id = $1 LIMIT 1;

-- name: ListApiLogs :many
SELECT * FROM api_logs
ORDER BY created_at DESC
LIMIT $1
OFFSET $2;

-- name: ListApiLogsByEndpoint :many
SELECT * FROM api_logs
WHERE endpoint = $1
ORDER BY created_at DESC;

-- name: ListApiLogsByUnit :many
SELECT * FROM api_logs
WHERE unit_guid = $1
ORDER BY created_at DESC;

-- name: GetApiStatistics :many
SELECT
    endpoint,
    COUNT(*) as request_count,
    AVG(response_time_ms)::int as avg_response_time,
    COUNT(CASE WHEN status_code >= 400 THEN 1 END) as error_count
FROM api_logs
WHERE created_at >= now() - interval '1 day'
GROUP BY endpoint
ORDER BY request_count DESC;

-- name: ListSlowRequests :many
SELECT * FROM api_logs
WHERE response_time_ms > $1
ORDER BY response_time_ms DESC;

-- name: ListApiErrors :many
SELECT * FROM api_logs
WHERE status_code >= $1
ORDER BY created_at DESC;

-- name: UpdateApiLog :one
UPDATE api_logs
SET
    response_time_ms = $2
WHERE id = $1
RETURNING *;

-- name: DeleteApiLog :exec
DELETE FROM api_logs
WHERE id = $1;

-- name: CleanupOldApiLogs :exec
DELETE FROM api_logs
WHERE created_at < now() - interval '30 days';