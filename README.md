# TSV Processing Service
Сервис на Go для автоматической обработки TSV-файлов, сохранения данных в PostgreSQL, генерации отчётов и REST API.

## Основные возможности
- **Автоматическая обработка TSV** — мониторинг директории incoming, парсинг файлов с данными устройств

- **Умный парсинг** — автоматическое определение UUID, корректное распределение полей независимо от разделителей

- **Worker pool** — параллельная обработка файлов (до 2 воркеров, настраивается)

- **PostgreSQL** — сохранение данных, ошибок, отчётов; генерация кода через sqlc

- **Генерация отчётов** — текстовые отчёты по каждому unit_guid в папку reports/

- **REST API** — получение данных с пагинацией, статусы файлов, ошибки, статистика

- **Graceful shutdown** — ожидание завершения обработки при остановке

Ниже перечень curl‑запросов, которыми можно прогнать весь happy-path сценарий: создание тестового файла, обработка, проверка данных, генерация отчёта, статистика.

# Health
curl -s http://localhost:8080/health

# Создаём тестовый TSV файл в директории incoming
cat > incoming/device_test.tsv << 'EOF'
n	mqtt	invid	unit_guid	msg_id	text	context	class	level	area	addr
1		G-044322	01749246-95f6-57db-b7c3-2ae0e8be671f	cold7_Defrost_status	Разморозка		waiting	100	LOCAL	cold7_status.Defrost_status
2		G-044322	01749246-95f6-57db-b7c3-2ae0e8be671f	cold7_VentSK_status	Вентилятор		working	100	LOCAL	cold7_status.VentSK_status
EOF

# Watcher автоматически обнаружит файл, поставит в очередь и обработает
# Проверим статус файлов
curl -s "http://localhost:8080/api/v1/files?page=1&limit=5"

# Данные устройства с пагинацией
curl -s "http://localhost:8080/api/v1/devices/01749246-95f6-57db-b7c3-2ae0e8be671f/data?page=1&limit=2"

# Ошибки файла (если есть)
curl -s "http://localhost:8080/api/v1/files/device_test.tsv/errors"

# Генерация отчёта по запросу
curl -s -X POST "http://localhost:8080/api/v1/reports/01749246-95f6-57db-b7c3-2ae0e8be671f/generate"

# Список отчётов по устройству
curl -s "http://localhost:8080/api/v1/reports/01749246-95f6-57db-b7c3-2ae0e8be671f"

# Общая статистика
curl -s "http://localhost:8080/api/v1/statistics"

# Принудительная обработка файла (если нужно повторно)
curl -s -X POST "http://localhost:8080/api/v1/files/device_test.tsv/process"

# Build & run everything (postgres + приложение + автоматический прогон go test)
docker compose up --build


