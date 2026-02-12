// internal/config/config.go

package config

import (
	"fmt"
	"log"
	"path/filepath"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"github.com/spf13/viper"
)

// AppConfig - главная структура конфигурации
type AppConfig struct {
	Database  DatabaseConfig  `mapstructure:"database"`
	Directory DirectoryConfig `mapstructure:"directory"`
	Server    ServerConfig    `mapstructure:"server"`
	Worker    WorkerConfig    `mapstructure:"worker"`
	Logging   LoggingConfig   `mapstructure:"logging"`
	Debug     bool            `mapstructure:"debug"` // ← Добавлено
}

// DatabaseConfig - конфигурация базы данных
type DatabaseConfig struct {
	Host         string        `mapstructure:"host"`
	Port         int           `mapstructure:"port"`
	User         string        `mapstructure:"user"`
	Password     string        `mapstructure:"password"`
	Name         string        `mapstructure:"name"`
	SSLMode      string        `mapstructure:"ssl_mode"`
	MaxOpenConns int           `mapstructure:"max_open_conns"`
	MaxIdleConns int           `mapstructure:"max_idle_conns"`
	MaxIdleTime  time.Duration `mapstructure:"max_idle_time"`
}

// DirectoryConfig - конфигурация директорий
type DirectoryConfig struct {
	WatchPath   string `mapstructure:"watch_path"`
	OutputPath  string `mapstructure:"output_path"`
	ArchivePath string `mapstructure:"archive_path"`
	ErrorPath   string `mapstructure:"error_path"`
	TempPath    string `mapstructure:"temp_path"`
}

// ServerConfig - конфигурация сервера
type ServerConfig struct {
	Host               string        `mapstructure:"host"`
	Port               int           `mapstructure:"port"`
	ReadTimeout        time.Duration `mapstructure:"read_timeout"`
	WriteTimeout       time.Duration `mapstructure:"write_timeout"`
	IdleTimeout        time.Duration `mapstructure:"idle_timeout"`
	ShutdownTimeout    time.Duration `mapstructure:"shutdown_timeout"`
	EnableCORS         bool          `mapstructure:"enable_cors"`
	CORSAllowedOrigins []string      `mapstructure:"cors_allowed_origins"`
}

// WorkerConfig - конфигурация воркеров
type WorkerConfig struct {
	MaxWorkers    int           `mapstructure:"max_workers"`
	MaxQueueSize  int           `mapstructure:"max_queue_size"`
	ScanInterval  time.Duration `mapstructure:"scan_interval"`
	RetryAttempts int           `mapstructure:"retry_attempts"`
	RetryDelay    time.Duration `mapstructure:"retry_delay"`
	BatchSize     int           `mapstructure:"batch_size"`
}

// LoggingConfig - конфигурация логирования
type LoggingConfig struct {
	Level      string `mapstructure:"level"`
	Format     string `mapstructure:"format"`
	Output     string `mapstructure:"output"`
	FilePath   string `mapstructure:"file_path"`
	MaxSizeMB  int    `mapstructure:"max_size_mb"`
	MaxBackups int    `mapstructure:"max_backups"`
	MaxAgeDays int    `mapstructure:"max_age_days"`
}

// LoadConfig - загружает конфигурацию из файла и переменных окружения
func LoadConfig(configPath string) (*AppConfig, error) {
	v := viper.New()

	// Установка значений по умолчанию
	setDefaults(v)

	// Конфигурация из файла
	if configPath != "" {
		v.SetConfigFile(configPath)
	} else {
		v.SetConfigName("config")
		v.SetConfigType("yaml")
		v.AddConfigPath(".")
		v.AddConfigPath("./configs")
		v.AddConfigPath("$HOME/.TSVProcessingService")
		v.AddConfigPath("/etc/TSVProcessingService")
	}

	// Чтение конфигурационного файла
	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			log.Println("Config file not found, using defaults and environment variables")
		} else {
			return nil, fmt.Errorf("error reading config file: %w", err)
		}
	}

	// Чтение переменных окружения
	v.SetEnvPrefix("TSV")
	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	// Привязка конкретных переменных окружения
	bindEnvVariables(v)

	// Загрузка .env файла
	_ = godotenv.Load()

	// Десериализация конфигурации
	var cfg AppConfig
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unable to decode config: %w", err)
	}

	// Валидация
	if err := validateConfig(&cfg); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}

	// Нормализация путей
	normalizePaths(&cfg)

	return &cfg, nil
}

// GetDSN - возвращает строку подключения к PostgreSQL
func (c *DatabaseConfig) GetDSN() string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		c.Host, c.Port, c.User, c.Password, c.Name, c.SSLMode)
}

// GetDSNWithoutCredentials - возвращает DSN без пароля (для логирования)
func (c *DatabaseConfig) GetDSNWithoutCredentials() string {
	return fmt.Sprintf("host=%s port=%d user=%s dbname=%s sslmode=%s",
		c.Host, c.Port, c.User, c.Name, c.SSLMode)
}

// GetListenAddr - возвращает адрес для прослушивания сервера
func (c *ServerConfig) GetListenAddr() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}

// GetWatchPath - возвращает путь к директории для мониторинга
func (c *DirectoryConfig) GetWatchPath() string {
	return c.WatchPath
}

// GetOutputPath - возвращает путь к директории для отчетов
func (c *DirectoryConfig) GetOutputPath() string {
	return c.OutputPath
}

// GetArchivePath - возвращает путь к директории архива
func (c *DirectoryConfig) GetArchivePath() string {
	return c.ArchivePath
}

// GetTempPath - возвращает путь к временной директории
func (c *DirectoryConfig) GetTempPath() string {
	return c.TempPath
}

// GetFileCheckInterval - возвращает интервал проверки файлов
func (c *WorkerConfig) GetFileCheckInterval() time.Duration {
	return c.ScanInterval
}

// GetMaxWorkers - возвращает максимальное количество воркеров
func (c *WorkerConfig) GetMaxWorkers() int {
	return c.MaxWorkers
}

// setDefaults - устанавливает значения по умолчанию
func setDefaults(v *viper.Viper) {
	// База данных
	v.SetDefault("database.host", "localhost")
	v.SetDefault("database.port", 5432)
	v.SetDefault("database.user", "postgres")
	v.SetDefault("database.password", "")
	v.SetDefault("database.name", "tsv_db")
	v.SetDefault("database.ssl_mode", "disable")
	v.SetDefault("database.max_open_conns", 25)
	v.SetDefault("database.max_idle_conns", 5)
	v.SetDefault("database.max_idle_time", "5m")

	// Директории
	v.SetDefault("directory.watch_path", "./incoming")
	v.SetDefault("directory.output_path", "./reports")
	v.SetDefault("directory.archive_path", "./archive")
	v.SetDefault("directory.temp_path", "./tmp")

	// Сервер
	v.SetDefault("server.host", "0.0.0.0")
	v.SetDefault("server.port", 8080)
	v.SetDefault("server.read_timeout", "15s")
	v.SetDefault("server.write_timeout", "15s")
	v.SetDefault("server.idle_timeout", "60s")
	v.SetDefault("server.shutdown_timeout", "10s")
	v.SetDefault("server.enable_cors", true)
	v.SetDefault("server.cors_allowed_origins", []string{"*"})

	// Воркеры
	v.SetDefault("worker.max_workers", 3)
	v.SetDefault("worker.max_queue_size", 100)
	v.SetDefault("worker.scan_interval", "30s")
	v.SetDefault("worker.retry_attempts", 3)
	v.SetDefault("worker.retry_delay", "10s")
	v.SetDefault("worker.batch_size", 1000)

	// Логирование
	v.SetDefault("logging.level", "info")
	v.SetDefault("logging.format", "json")
	v.SetDefault("logging.output", "stdout")
	v.SetDefault("logging.file_path", "./logs/TSVProcessingService.log")
	v.SetDefault("logging.max_size_mb", 100)
	v.SetDefault("logging.max_backups", 3)
	v.SetDefault("logging.max_age_days", 30)

	// Отладка
	v.SetDefault("debug", false)
}

// validateConfig - валидирует конфигурацию
func validateConfig(cfg *AppConfig) error {
	var errors []string

	if cfg.Database.Host == "" {
		errors = append(errors, "database.host is required")
	}
	if cfg.Database.Port <= 0 || cfg.Database.Port > 65535 {
		errors = append(errors, "database.port must be between 1 and 65535")
	}
	if cfg.Database.Name == "" {
		errors = append(errors, "database.name is required")
	}
	if cfg.Directory.WatchPath == "" {
		errors = append(errors, "directory.watch_path is required")
	}
	if cfg.Directory.OutputPath == "" {
		errors = append(errors, "directory.output_path is required")
	}
	if cfg.Worker.MaxWorkers <= 0 {
		errors = append(errors, "worker.max_workers must be greater than 0")
	}
	if cfg.Worker.ScanInterval <= 0 {
		errors = append(errors, "worker.scan_interval must be greater than 0")
	}

	if len(errors) > 0 {
		return fmt.Errorf("config validation errors: %s", strings.Join(errors, ", "))
	}

	return nil
}

// normalizePaths - нормализует пути (делает их абсолютными)
func normalizePaths(cfg *AppConfig) {
	cfg.Directory.WatchPath = normalizePath(cfg.Directory.WatchPath)
	cfg.Directory.OutputPath = normalizePath(cfg.Directory.OutputPath)
	cfg.Directory.ArchivePath = normalizePath(cfg.Directory.ArchivePath)
	cfg.Directory.TempPath = normalizePath(cfg.Directory.TempPath)
	cfg.Logging.FilePath = normalizePath(cfg.Logging.FilePath)
}

// normalizePath - преобразует относительный путь в абсолютный
func normalizePath(path string) string {
	if path == "" || filepath.IsAbs(path) {
		return path
	}

	if absPath, err := filepath.Abs(path); err == nil {
		return absPath
	}

	return path
}

// PrintConfig - выводит конфигурацию (без секретов)
func (c *AppConfig) PrintConfig() {
	log.Println("=== Loaded Configuration ===")
	log.Printf("Database: host=%s, port=%d, name=%s", c.Database.Host, c.Database.Port, c.Database.Name)
	log.Printf("Directories: watch=%s, output=%s", c.Directory.WatchPath, c.Directory.OutputPath)
	log.Printf("Server: listen=%s:%d", c.Server.Host, c.Server.Port)
	log.Printf("Workers: max=%d, scan_interval=%v", c.Worker.MaxWorkers, c.Worker.ScanInterval)
	log.Printf("Logging: level=%s, format=%s", c.Logging.Level, c.Logging.Format)
	log.Println("===========================")
}

// IsDebugMode - проверяет, включен ли режим отладки
func (c *AppConfig) IsDebugMode() bool {
	return c.Debug || c.Logging.Level == "debug"
}

// bindEnvVariables - привязывает переменные окружения
func bindEnvVariables(v *viper.Viper) {
	bind := func(key, env string) {
		if err := v.BindEnv(key, env); err != nil {
			log.Printf("Warning: failed to bind env variable %s to %s: %v", env, key, err)
		}
	}

	// База данных
	bind("database.host", "TSV_DATABASE_HOST")
	bind("database.port", "TSV_DATABASE_PORT")
	bind("database.user", "TSV_DATABASE_USER")
	bind("database.password", "TSV_DATABASE_PASSWORD")
	bind("database.name", "TSV_DATABASE_NAME")
	bind("database.ssl_mode", "TSV_DATABASE_SSL_MODE")

	// Директории
	bind("directory.watch_path", "TSV_DIRECTORY_WATCH_PATH")
	bind("directory.output_path", "TSV_DIRECTORY_OUTPUT_PATH")
	bind("directory.archive_path", "TSV_DIRECTORY_ARCHIVE_PATH")

	// Сервер
	bind("server.host", "TSV_SERVER_HOST")
	bind("server.port", "TSV_SERVER_PORT")

	// Логирование
	bind("logging.level", "TSV_LOGGING_LEVEL")
	bind("logging.format", "TSV_LOGGING_FORMAT")

	// Отладка
	bind("debug", "TSV_DEBUG")
}
