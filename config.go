package main

import (
	"log"
	"os"
	"path/filepath"

	"github.com/joho/godotenv"
	"gopkg.in/yaml.v3"
)

// AdminConfig controls who can manage books (upload/remove/scan).
// Role names and IDs are checked against the user's Discord roles.
// User IDs grant admin access to specific users regardless of role.
type AdminConfig struct {
	RoleNames []string `yaml:"role_names"` // matched case-insensitively
	RoleIDs   []string `yaml:"role_ids"`   // Discord snowflake IDs (more reliable)
	UserIDs   []string `yaml:"user_ids"`   // specific Discord user snowflake IDs
}

type yamlConfig struct {
	Admin AdminConfig `yaml:"admin"`
}

type Config struct {
	DiscordToken   string
	DiscordGuildID string
	AnthropicKey   string
	Admin          AdminConfig
	DBPath         string
	PDFDir         string
}

// LoadConfig loads configuration from the data directory.
//   - Secrets  → <dataDir>/.env
//   - Settings → <dataDir>/config.yaml
//
// DB and PDF paths default to subdirectories of dataDir.
func LoadConfig(dataDir string) Config {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		log.Fatalf("create data dir %s: %v", dataDir, err)
	}

	// Load secrets from .env (ignore error — env vars may already be set)
	_ = godotenv.Load(filepath.Join(dataDir, ".env"))

	// Load settings from config.yaml
	adminCfg := loadYAMLConfig(filepath.Join(dataDir, "config.yaml"))

	cfg := Config{
		DiscordToken:   os.Getenv("DISCORD_TOKEN"),
		DiscordGuildID: os.Getenv("DISCORD_GUILD_ID"),
		AnthropicKey:   os.Getenv("ANTHROPIC_API_KEY"),
		Admin:          adminCfg,
		DBPath:         getEnvDefault("DB_PATH", filepath.Join(dataDir, "rules.db")),
		PDFDir:         getEnvDefault("PDF_DIR", filepath.Join(dataDir, "pdfs")),
	}

	if cfg.DiscordToken == "" {
		log.Fatal("DISCORD_TOKEN is required (set in <data-dir>/.env)")
	}
	if cfg.AnthropicKey == "" {
		log.Fatal("ANTHROPIC_API_KEY is required (set in <data-dir>/.env)")
	}

	return cfg
}

// loadYAMLConfig reads config.yaml. If the file doesn't exist it writes a
// default one and returns the default config so the bot still starts.
func loadYAMLConfig(path string) AdminConfig {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		writeDefaultYAML(path)
		return defaultAdminConfig()
	}
	if err != nil {
		log.Fatalf("read %s: %v", path, err)
	}

	var yc yamlConfig
	if err := yaml.Unmarshal(data, &yc); err != nil {
		log.Fatalf("parse %s: %v", path, err)
	}

	// If the file exists but admin section is empty, use defaults
	if len(yc.Admin.RoleNames) == 0 && len(yc.Admin.RoleIDs) == 0 && len(yc.Admin.UserIDs) == 0 {
		return defaultAdminConfig()
	}
	return yc.Admin
}

func defaultAdminConfig() AdminConfig {
	return AdminConfig{
		RoleNames: []string{"DM"},
	}
}

func writeDefaultYAML(path string) {
	const defaultContent = `# Rules Lawyer — bot configuration
# Edit this file to control who can manage rulebooks.

admin:
  # Role names (case-insensitive). Users with any of these roles can
  # upload, remove, and scan books.
  role_names:
    - DM

  # Role IDs (Discord snowflakes). More reliable than names since they
  # don't break if a role is renamed. Right-click a role → Copy Role ID.
  role_ids: []

  # User IDs. These specific users always have admin access regardless
  # of their roles. Right-click a user → Copy User ID.
  user_ids: []
`
	if err := os.WriteFile(path, []byte(defaultContent), 0644); err != nil {
		log.Printf("warn: could not write default config.yaml: %v", err)
	} else {
		log.Printf("created default config at %s — edit it to configure admins", path)
	}
}

func getEnvDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
