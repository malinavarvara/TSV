-- name: CreateDeviceData :one
INSERT INTO device_data (
    file_id,
    unit_guid,
    mqtt,
    invid,
    msg_id,
    text,
    context,
    class,
    level,
    area,
    addr,
    block,
    type,
    bit,
    invert_bit,
    line_number
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
) RETURNING *;

-- name: BulkInsertDeviceData :exec
INSERT INTO device_data (
    file_id,
    unit_guid,
    mqtt,
    invid,
    msg_id,
    text,
    context,
    class,
    level,
    area,
    addr,
    block,
    type,
    bit,
    invert_bit,
    line_number
) VALUES 
    ( $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16 ),
    ( $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32 )
RETURNING *;

-- name: GetDeviceDataByID :one
SELECT * FROM device_data
WHERE id = $1 LIMIT 1;

-- name: GetDeviceDataByFileID :many
SELECT * FROM device_data
WHERE file_id = $1
ORDER BY line_number;

-- name: ListDeviceDataByUnit :many
SELECT * FROM device_data
WHERE unit_guid = $1
ORDER BY created_at DESC
LIMIT $2
OFFSET $3;

-- name: ListDeviceDataByClass :many
SELECT * FROM device_data
WHERE class = $1 AND file_id = $2
ORDER BY line_number;

-- name: SearchDeviceDataText :many
SELECT * FROM device_data
WHERE text ILIKE '%' || $1 || '%'
AND file_id = $2
ORDER BY line_number;

-- name: GetDeviceStatistics :one
SELECT
    COUNT(*) as total_records,
    COUNT(DISTINCT unit_guid) as unique_devices,
    COUNT(DISTINCT class) as unique_classes,
    MIN(created_at) as first_record,
    MAX(created_at) as last_record
FROM device_data
WHERE file_id = $1;

-- name: UpdateDeviceData :one
UPDATE device_data
SET
    text = $2,
    level = $3,
    class = $4
WHERE id = $1
RETURNING *;

-- name: DeleteDeviceData :exec
DELETE FROM device_data
WHERE id = $1;

-- name: DeleteDeviceDataByFileID :exec
DELETE FROM device_data
WHERE file_id = $1;