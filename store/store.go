package store

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	_ "modernc.org/sqlite"
)

// currentSchemaVersion is bumped whenever a breaking schema change is made.
// The migrate() function runs all pending migrations in order.
const currentSchemaVersion = 4

type Book struct {
	ID       int64
	Name     string
	Filename string
	Edition  string
	AddedAt  string
}

type Chunk struct {
	BookName string
	Edition  string
	Page     int
	Section  string // current section heading (may be empty)
	Content  string
}

type Store struct {
	db *sql.DB
}

func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}

	// Enable WAL mode for concurrent read/write access, set a busy timeout
	// so concurrent writers retry instead of failing immediately, and turn on
	// foreign key enforcement.
	for _, pragma := range []string{
		"PRAGMA journal_mode = WAL",
		"PRAGMA busy_timeout = 5000",
		"PRAGMA foreign_keys = ON",
	} {
		if _, err := db.Exec(pragma); err != nil {
			db.Close()
			return nil, fmt.Errorf("%s: %w", pragma, err)
		}
	}

	if err := migrate(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}

	return &Store{db: db}, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func migrate(db *sql.DB) error {
	// Schema version tracking table
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL DEFAULT 0)`); err != nil {
		return err
	}
	if _, err := db.Exec(`INSERT INTO schema_version (version) SELECT 0 WHERE NOT EXISTS (SELECT 1 FROM schema_version)`); err != nil {
		return err
	}

	var version int
	if err := db.QueryRow(`SELECT version FROM schema_version`).Scan(&version); err != nil {
		return err
	}

	// v1: initial schema — books table + basic FTS5 chunks
	if version < 1 {
		if _, err := db.Exec(`
			CREATE TABLE IF NOT EXISTS books (
				id        INTEGER PRIMARY KEY AUTOINCREMENT,
				name      TEXT NOT NULL UNIQUE,
				filename  TEXT NOT NULL,
				edition   TEXT NOT NULL DEFAULT 'unknown',
				added_at  DATETIME DEFAULT CURRENT_TIMESTAMP
			);
		`); err != nil {
			return err
		}
		if _, err := db.Exec(`
			CREATE VIRTUAL TABLE IF NOT EXISTS chunks USING fts5(
				book_name,
				content,
				book_id   UNINDEXED,
				page      UNINDEXED,
				edition   UNINDEXED
			);
		`); err != nil {
			return err
		}
		if _, err := db.Exec(`UPDATE schema_version SET version = 1`); err != nil {
			return err
		}
		version = 1
	}

	// v2: better FTS5 tokenizer (unicode61 + remove_diacritics).
	// The old chunks are dropped — books with bad OCR text must be re-indexed.
	if version < 2 {
		log.Println("store: migrating to schema v2 — dropping old chunks (re-index all books)")
		if _, err := db.Exec(`DROP TABLE IF EXISTS chunks`); err != nil {
			return err
		}
		if _, err := db.Exec(`UPDATE schema_version SET version = 2`); err != nil {
			return err
		}
		version = 2
	}

	// v3: replace monolithic FTS5 table with a real chunks table + FTS5 content
	// table backed by it. Adds section column. Books table is preserved.
	if version < 3 {
		log.Println("store: migrating to schema v3 — structured chunks + FTS5 content table (re-index all books)")
		if _, err := db.Exec(`DROP TABLE IF EXISTS chunks`); err != nil {
			return err
		}
		if _, err := db.Exec(`DROP TABLE IF EXISTS chunks_fts`); err != nil {
			return err
		}
		if _, err := db.Exec(`
			CREATE TABLE IF NOT EXISTS chunks (
				id        INTEGER PRIMARY KEY AUTOINCREMENT,
				book_id   INTEGER NOT NULL,
				book_name TEXT    NOT NULL,
				edition   TEXT    NOT NULL,
				page      INTEGER NOT NULL,
				section   TEXT    NOT NULL DEFAULT '',
				content   TEXT    NOT NULL
			);
		`); err != nil {
			return err
		}
		if _, err := db.Exec(`
			CREATE VIRTUAL TABLE IF NOT EXISTS chunks_fts USING fts5(
				content,
				section,
				book_name,
				content     = 'chunks',
				content_rowid = 'id',
				tokenize    = 'unicode61 remove_diacritics 1'
			);
		`); err != nil {
			return err
		}
		if _, err := db.Exec(`
			CREATE TRIGGER IF NOT EXISTS chunks_ai AFTER INSERT ON chunks BEGIN
				INSERT INTO chunks_fts(rowid, content, section, book_name)
				VALUES (new.id, new.content, new.section, new.book_name);
			END;
		`); err != nil {
			return err
		}
		if _, err := db.Exec(`
			CREATE TRIGGER IF NOT EXISTS chunks_ad AFTER DELETE ON chunks BEGIN
				INSERT INTO chunks_fts(chunks_fts, rowid, content, section, book_name)
				VALUES ('delete', old.id, old.content, old.section, old.book_name);
			END;
		`); err != nil {
			return err
		}
		if _, err := db.Exec(`UPDATE schema_version SET version = 3`); err != nil {
			return err
		}
		version = 3
	}

	// v4: normalise chunks — remove denormalised book_name/edition columns,
	// add foreign key with ON DELETE CASCADE, add index on book_id, and
	// rebuild FTS5 to only index content + section.
	// Existing chunk data is dropped; books must be re-indexed.
	if version < 4 {
		log.Println("store: migrating to schema v4 — normalised chunks + FK cascade (re-index all books)")

		// Drop old tables/triggers
		for _, stmt := range []string{
			`DROP TRIGGER IF EXISTS chunks_ai`,
			`DROP TRIGGER IF EXISTS chunks_ad`,
			`DROP TABLE IF EXISTS chunks_fts`,
			`DROP TABLE IF EXISTS chunks`,
		} {
			if _, err := db.Exec(stmt); err != nil {
				return err
			}
		}

		// Recreate chunks with FK and no denormalised columns
		if _, err := db.Exec(`
			CREATE TABLE chunks (
				id      INTEGER PRIMARY KEY AUTOINCREMENT,
				book_id INTEGER NOT NULL REFERENCES books(id) ON DELETE CASCADE,
				page    INTEGER NOT NULL,
				section TEXT    NOT NULL DEFAULT '',
				content TEXT    NOT NULL
			)
		`); err != nil {
			return err
		}

		if _, err := db.Exec(`CREATE INDEX idx_chunks_book_id ON chunks(book_id)`); err != nil {
			return err
		}

		// FTS5 external-content table backed by chunks
		if _, err := db.Exec(`
			CREATE VIRTUAL TABLE chunks_fts USING fts5(
				content,
				section,
				content     = 'chunks',
				content_rowid = 'id',
				tokenize    = 'unicode61 remove_diacritics 1'
			)
		`); err != nil {
			return err
		}

		// Sync triggers
		if _, err := db.Exec(`
			CREATE TRIGGER chunks_ai AFTER INSERT ON chunks BEGIN
				INSERT INTO chunks_fts(rowid, content, section)
				VALUES (new.id, new.content, new.section);
			END
		`); err != nil {
			return err
		}
		if _, err := db.Exec(`
			CREATE TRIGGER chunks_ad AFTER DELETE ON chunks BEGIN
				INSERT INTO chunks_fts(chunks_fts, rowid, content, section)
				VALUES ('delete', old.id, old.content, old.section);
			END
		`); err != nil {
			return err
		}

		// Also update books table: allow same name with different editions
		// by replacing the UNIQUE(name) constraint with UNIQUE(name, edition).
		// SQLite can't ALTER constraints, so rebuild the table.
		if _, err := db.Exec(`
			CREATE TABLE books_new (
				id        INTEGER PRIMARY KEY AUTOINCREMENT,
				name      TEXT NOT NULL,
				filename  TEXT NOT NULL,
				edition   TEXT NOT NULL DEFAULT 'unknown',
				added_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
				UNIQUE(name, edition)
			)
		`); err != nil {
			return err
		}
		if _, err := db.Exec(`INSERT INTO books_new (id, name, filename, edition, added_at) SELECT id, name, filename, edition, added_at FROM books`); err != nil {
			return err
		}
		if _, err := db.Exec(`DROP TABLE books`); err != nil {
			return err
		}
		if _, err := db.Exec(`ALTER TABLE books_new RENAME TO books`); err != nil {
			return err
		}

		if _, err := db.Exec(`UPDATE schema_version SET version = 4`); err != nil {
			return err
		}
		version = 4
	}

	return nil
}

// BookExists returns true if a book with the given name is already indexed.
func (s *Store) BookExists(name string) (bool, error) {
	var exists bool
	err := s.db.QueryRow(`SELECT EXISTS(SELECT 1 FROM books WHERE name = ?)`, name).Scan(&exists)
	return exists, err
}

// AddBook inserts a book record and returns its ID.
func (s *Store) AddBook(name, filename, edition string) (int64, error) {
	res, err := s.db.Exec(
		`INSERT INTO books (name, filename, edition) VALUES (?, ?, ?)`,
		name, filename, edition,
	)
	if err != nil {
		return 0, fmt.Errorf("insert book: %w", err)
	}
	return res.LastInsertId()
}

// AddChunks bulk-inserts chunks for a book into the chunks table.
// The chunks_fts FTS5 table is kept in sync automatically via triggers.
func (s *Store) AddChunks(bookID int64, chunks []Chunk) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT INTO chunks (book_id, page, section, content)
		VALUES (?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, c := range chunks {
		if _, err := stmt.Exec(bookID, c.Page, c.Section, c.Content); err != nil {
			return fmt.Errorf("insert chunk p%d: %w", c.Page, err)
		}
	}
	return tx.Commit()
}

// SearchChunks performs an FTS5 search and returns the top results.
// If edition is non-empty, results are filtered to that edition.
// Strategy: try AND (all words must match), fall back to OR (any word matches)
// so that conversational questions like "what is a saving throw?" still find results.
func (s *Store) SearchChunks(query, edition string, limit int) ([]Chunk, error) {
	andQuery := sanitizeFTS(query, "AND")
	orQuery := sanitizeFTS(query, "OR")

	results, err := s.runSearch(andQuery, edition, limit)
	if err != nil {
		return nil, err
	}
	if len(results) > 0 {
		return results, nil
	}
	// AND matched nothing — widen to OR
	return s.runSearch(orQuery, edition, limit)
}

func (s *Store) runSearch(ftsQuery, edition string, limit int) ([]Chunk, error) {
	var (
		rows *sql.Rows
		err  error
	)

	// bm25 column weights: content=1.0, section=10.0
	// This heavily boosts chunks where the search term appears in a section
	// heading so that definitional chapters outrank passing mentions.
	if edition != "" {
		rows, err = s.db.Query(`
			SELECT b.name, b.edition, c.page, c.section, c.content
			FROM chunks c
			JOIN chunks_fts f ON c.id = f.rowid
			JOIN books b ON c.book_id = b.id
			WHERE chunks_fts MATCH ? AND b.edition = ?
			ORDER BY bm25(chunks_fts, 1.0, 10.0)
			LIMIT ?
		`, ftsQuery, edition, limit)
	} else {
		rows, err = s.db.Query(`
			SELECT b.name, b.edition, c.page, c.section, c.content
			FROM chunks c
			JOIN chunks_fts f ON c.id = f.rowid
			JOIN books b ON c.book_id = b.id
			WHERE chunks_fts MATCH ?
			ORDER BY bm25(chunks_fts, 1.0, 10.0)
			LIMIT ?
		`, ftsQuery, limit)
	}
	if err != nil {
		// FTS5 syntax errors should not propagate as hard errors
		return nil, fmt.Errorf("fts search: %w", err)
	}
	defer rows.Close()

	var results []Chunk
	for rows.Next() {
		var c Chunk
		if err := rows.Scan(&c.BookName, &c.Edition, &c.Page, &c.Section, &c.Content); err != nil {
			return nil, err
		}
		results = append(results, c)
	}
	return results, rows.Err()
}

// ListBooks returns all indexed books.
func (s *Store) ListBooks() ([]Book, error) {
	rows, err := s.db.Query(`SELECT id, name, filename, edition, added_at FROM books ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var books []Book
	for rows.Next() {
		var b Book
		if err := rows.Scan(&b.ID, &b.Name, &b.Filename, &b.Edition, &b.AddedAt); err != nil {
			return nil, err
		}
		books = append(books, b)
	}
	return books, rows.Err()
}

// RemoveBook deletes a book by name. Associated chunks are removed
// automatically via the ON DELETE CASCADE foreign key constraint, and the
// chunks_ad trigger keeps the FTS5 index in sync.
func (s *Store) RemoveBook(name string) (bool, error) {
	res, err := s.db.Exec(`DELETE FROM books WHERE name = ?`, name)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// RemoveAllBooks deletes every book and all associated chunks.
// Returns the number of books removed.
func (s *Store) RemoveAllBooks() (int64, error) {
	res, err := s.db.Exec(`DELETE FROM books`)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// stopWords are common English words that add noise to FTS5 queries.
// Removing them lets content words like "changeling" match without
// requiring filler words ("what", "are") to also appear in the chunk.
var stopWords = map[string]bool{
	"a": true, "an": true, "the": true, "and": true, "or": true,
	"but": true, "is": true, "are": true, "was": true, "were": true,
	"be": true, "been": true, "being": true, "am": true,
	"do": true, "does": true, "did": true,
	"have": true, "has": true, "had": true,
	"will": true, "would": true, "shall": true, "should": true,
	"can": true, "could": true, "may": true, "might": true, "must": true,
	"not": true, "no": true,
	"i": true, "me": true, "my": true, "we": true, "our": true, "you": true, "your": true,
	"he": true, "she": true, "it": true, "its": true, "they": true, "them": true, "their": true,
	"this": true, "that": true, "these": true, "those": true,
	"what": true, "which": true, "who": true, "whom": true, "how": true, "when": true, "where": true, "why": true,
	"in": true, "on": true, "at": true, "to": true, "for": true, "of": true,
	"with": true, "by": true, "from": true, "about": true, "into": true,
	"if": true, "then": true, "so": true, "as": true, "than": true,
	"tell": true, "explain": true, "describe": true,
}

// sanitizeFTS builds an FTS5 query joining all content words with the given
// operator ("AND" or "OR"). Stop words are removed so that natural-language
// questions don't dilute the search. All non-alphanumeric characters are
// replaced with spaces to prevent FTS5 syntax errors.
func sanitizeFTS(q, op string) string {
	q = strings.TrimSpace(q)
	// Replace anything that isn't a letter, digit, or space with a space
	var sb strings.Builder
	for _, r := range q {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == ' ' {
			sb.WriteRune(r)
		} else {
			sb.WriteByte(' ')
		}
	}
	raw := strings.Fields(sb.String())
	var words []string
	for _, w := range raw {
		if !stopWords[strings.ToLower(w)] {
			words = append(words, w)
		}
	}
	// If all words were stop words, fall back to original words
	if len(words) == 0 {
		words = raw
	}
	if len(words) == 0 {
		return `""`
	}
	return strings.Join(words, " "+op+" ")
}
