# ---- Build stage ----
FROM golang:1.25-alpine AS builder

WORKDIR /app

# Кэширование модулей
COPY go.mod go.sum ./
RUN go mod download

# Копируем весь исходный код
COPY . .

# Сборка бинарника (ваша точка входа — cmd/api/main.go)
RUN CGO_ENABLED=0 GOOS=linux go build -o tsv-service ./cmd/api

# ---- Runtime stage ----
FROM alpine:latest

WORKDIR /root/

# Сертификаты и полезные утилиты
RUN apk --no-cache add ca-certificates tzdata

# Бинарник из builder
COPY --from=builder /app/tsv-service .

# Конфигурационный файл (можно не копировать, если используете только ENV)
COPY configs/config.yaml .

# Миграции (оставляем в образе для возможности запуска migrate из контейнера)
COPY db/migration ./db/migration

# Создаём рабочие директории (будут перекрыты volumes из compose)
RUN mkdir -p /incoming /reports /archive /tmp

# Порт из конфига (по умолчанию 8080)
EXPOSE 8080

# Запускаем сервис
CMD ["./tsv-service"]