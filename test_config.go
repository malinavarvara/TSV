package main

import (
	"TSVProcessingService/internal/config"
	"fmt"
	"log"
)

func main() {
	fmt.Println("=== Testing Configuration ===")

	// Загрузка конфигурации
	cfg, err := config.LoadConfig("configs/config.yaml")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Вывод информации
	fmt.Printf("✅ Config loaded successfully!\n\n")
	fmt.Printf("Database Configuration:\n")
	fmt.Printf("  Host: %s\n", cfg.Database.Host)
	fmt.Printf("  Port: %d\n", cfg.Database.Port)
	fmt.Printf("  User: %s\n", cfg.Database.User)
	fmt.Printf("  Name: %s\n", cfg.Database.Name)
	fmt.Printf("  SSL Mode: %s\n", cfg.Database.SSLMode)

	fmt.Printf("\nDirectories:\n")
	fmt.Printf("  Watch: %s\n", cfg.Directory.WatchPath)
	fmt.Printf("  Output: %s\n", cfg.Directory.OutputPath)
	fmt.Printf("  Archive: %s\n", cfg.Directory.ArchivePath)

	fmt.Printf("\nServer Configuration:\n")
	fmt.Printf("  Host: %s\n", cfg.Server.Host)
	fmt.Printf("  Port: %d\n", cfg.Server.Port)

	fmt.Printf("\nWorker Configuration:\n")
	fmt.Printf("  Max Workers: %d\n", cfg.Worker.MaxWorkers)
	fmt.Printf("  Scan Interval: %v\n", cfg.Worker.ScanInterval)

	fmt.Printf("\nLogging:\n")
	fmt.Printf("  Level: %s\n", cfg.Logging.Level)
	fmt.Printf("  Format: %s\n", cfg.Logging.Format)

	fmt.Printf("\nDebug Mode: %v\n", cfg.IsDebugMode())

	// Проверка DSN
	fmt.Printf("\nDatabase Connection String (without password):\n")
	fmt.Printf("  %s\n", cfg.Database.GetDSNWithoutCredentials())

	fmt.Println("\n✅ All tests passed!")
}
