package main

import (
	"log"
	"os"
	"path/filepath"

	"github.com/joho/godotenv"
)

type Config struct {
	DiscordToken   string
	DiscordGuildID string // optional: for instant slash command registration
	AnthropicKey   string
	AdminRoleName  string
	DBPath         string
	PDFDir         string
}

// LoadConfig loads configuration from the data directory.
// It reads <dataDir>/.env if present, then falls back to environment variables.
// DB and PDF paths default to subdirectories of dataDir unless overridden by env vars.
func LoadConfig(dataDir string) Config {
	// Ensure data directory exists
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		log.Fatalf("create data dir %s: %v", dataDir, err)
	}

	// Load .env from the data directory if present
	_ = godotenv.Load(filepath.Join(dataDir, ".env"))

	cfg := Config{
		DiscordToken:   os.Getenv("DISCORD_TOKEN"),
		DiscordGuildID: os.Getenv("DISCORD_GUILD_ID"),
		AnthropicKey:   os.Getenv("ANTHROPIC_API_KEY"),
		AdminRoleName:  getEnvDefault("ADMIN_ROLE_NAME", "DM"),
		DBPath:         getEnvDefault("DB_PATH", filepath.Join(dataDir, "rules.db")),
		PDFDir:         getEnvDefault("PDF_DIR", filepath.Join(dataDir, "pdfs")),
	}

	if cfg.DiscordToken == "" {
		log.Fatal("DISCORD_TOKEN is required")
	}
	if cfg.AnthropicKey == "" {
		log.Fatal("ANTHROPIC_API_KEY is required")
	}

	return cfg
}

func getEnvDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
