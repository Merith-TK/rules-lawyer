package bot

import (
	"fmt"
	"log"

	"github.com/bwmarrin/discordgo"

	"rules-laywer/store"
)

// AdminConfig mirrors the top-level AdminConfig so the bot package stays
// independent of the main package.
type AdminConfig struct {
	RoleNames []string
	RoleIDs   []string
	UserIDs   []string
}

// Bot holds all runtime state for the Discord bot.
type Bot struct {
	session      *discordgo.Session
	store        *store.Store
	anthropicKey string
	admin        AdminConfig
	pdfDir       string
	guildID      string

	registeredCmds []*discordgo.ApplicationCommand
}

var slashCommands = []*discordgo.ApplicationCommand{
	{
		Name:        "ask",
		Description: "Ask a rules question based on indexed rulebooks",
		Options: []*discordgo.ApplicationCommandOption{
			{
				Type:        discordgo.ApplicationCommandOptionString,
				Name:        "question",
				Description: "Your rules question",
				Required:    true,
			},
			{
				Type:        discordgo.ApplicationCommandOptionString,
				Name:        "edition",
				Description: "Filter to a specific edition (e.g. 5e2014, 5e2024, pathfinder2e)",
				Required:    false,
			},
		},
	},
	{
		Name:        "books",
		Description: "List all indexed rulebooks",
	},
	{
		Name:        "upload",
		Description: "Index a PDF rulebook (admin only — attach PDF or provide URL)",
		Options: []*discordgo.ApplicationCommandOption{
			{
				Type:        discordgo.ApplicationCommandOptionAttachment,
				Name:        "file",
				Description: "PDF file to upload",
				Required:    false,
			},
			{
				Type:        discordgo.ApplicationCommandOptionString,
				Name:        "url",
				Description: "URL to a PDF (alternative to attachment)",
				Required:    false,
			},
			{
				Type:        discordgo.ApplicationCommandOptionString,
				Name:        "name",
				Description: "Override the book name",
				Required:    false,
			},
			{
				Type:        discordgo.ApplicationCommandOptionString,
				Name:        "edition",
				Description: "Override edition detection (e.g. 5e2014, 5e2024)",
				Required:    false,
			},
		},
	},
	{
		Name:        "scan",
		Description: "Scan the PDF directory for new books to index (admin only)",
	},
	{
		Name:        "remove",
		Description: "Remove an indexed rulebook (admin only)",
		Options: []*discordgo.ApplicationCommandOption{
			{
				Type:        discordgo.ApplicationCommandOptionString,
				Name:        "book",
				Description: "Exact name of the book to remove",
				Required:    true,
			},
		},
	},
}

// New creates a new Bot instance and connects the Discord session.
func New(token, anthropicKey string, admin AdminConfig, pdfDir, guildID string, s *store.Store) (*Bot, error) {
	dg, err := discordgo.New("Bot " + token)
	if err != nil {
		return nil, fmt.Errorf("create discord session: %w", err)
	}

	b := &Bot{
		session:      dg,
		store:        s,
		anthropicKey: anthropicKey,
		admin:        admin,
		pdfDir:       pdfDir,
		guildID:      guildID,
	}

	dg.AddHandler(b.onReady)
	dg.AddHandler(b.onInteraction)
	dg.AddHandler(b.onMessage)

	// MESSAGE_CONTENT is a privileged intent that must also be enabled in the
	// Discord Developer Portal (Bot → Privileged Gateway Intents).
	// Without it, slash commands still work; prefix (!) commands will not.
	dg.Identify.Intents = discordgo.IntentsGuildMessages | discordgo.IntentMessageContent

	return b, nil
}

// Open opens the Discord websocket connection.
func (b *Bot) Open() error {
	return b.session.Open()
}

// Close gracefully removes slash commands and closes the session.
func (b *Bot) Close() {
	for _, cmd := range b.registeredCmds {
		if err := b.session.ApplicationCommandDelete(b.session.State.User.ID, b.guildID, cmd.ID); err != nil {
			log.Printf("warn: failed to delete command %s: %v", cmd.Name, err)
		}
	}
	b.session.Close()
}

func (b *Bot) onReady(s *discordgo.Session, r *discordgo.Ready) {
	log.Printf("Logged in as %s#%s (ID: %s)", r.User.Username, r.User.Discriminator, r.User.ID)
	log.Printf("Invite URL: %s", inviteURL(r.User.ID))
	log.Printf("Admin config — userIDs=%v roleIDs=%v roleNames=%v", b.admin.UserIDs, b.admin.RoleIDs, b.admin.RoleNames)
	b.registerSlashCommands(s)
}

func (b *Bot) registerSlashCommands(s *discordgo.Session) {
	for _, cmd := range slashCommands {
		registered, err := s.ApplicationCommandCreate(s.State.User.ID, b.guildID, cmd)
		if err != nil {
			log.Printf("error: register slash command %s: %v", cmd.Name, err)
			continue
		}
		b.registeredCmds = append(b.registeredCmds, registered)
		log.Printf("registered slash command: /%s", cmd.Name)
	}
}

// inviteURL builds the OAuth2 URL to add the bot to a server.
// Permissions: Send Messages (2048) + Read Message History (65536) + Embed Links (16384)
func inviteURL(clientID string) string {
	const permissions = 2048 + 65536 + 16384 // 83968
	return fmt.Sprintf(
		"https://discord.com/api/oauth2/authorize?client_id=%s&permissions=%d&scope=bot+applications.commands",
		clientID, permissions,
	)
}
