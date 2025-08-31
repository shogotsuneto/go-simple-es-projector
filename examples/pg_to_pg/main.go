// Package main demonstrates a complete PostgreSQL-to-PostgreSQL projection example
// using database/sql with inline transactions.
//
// This example shows:
// - Loading a starting cursor from user-managed storage
// - Running the projector with user-defined Apply function
// - Atomic projection + checkpoint persistence using database transactions
// - Restarting from the saved cursor without re-applying past events
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/shogotsuneto/go-simple-es-projector"
	es "github.com/shogotsuneto/go-simple-eventstore"
	_ "github.com/lib/pq" // PostgreSQL driver
)

// Example event types
type CardCreated struct {
	CardID string `json:"card_id"`
	UserID string `json:"user_id"`
	Title  string `json:"title"`
}

type CardUpdated struct {
	CardID string `json:"card_id"`
	Title  string `json:"title"`
}

func main() {
	// Example usage - in real code, get from environment/config
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://user:password@localhost/projector_example?sslmode=disable"
		log.Printf("Using default DATABASE_URL: %s", dbURL)
	}

	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Verify connection
	if err := db.Ping(); err != nil {
		log.Printf("Database connection failed: %v", err)
		log.Printf("Skipping example - requires PostgreSQL")
		return
	}

	// Create tables (in real code, use migrations)
	if err := createTables(context.Background(), db); err != nil {
		log.Fatalf("Failed to create tables: %v", err)
	}

	// Create event source (assuming go-simple-eventstore has a PostgreSQL consumer)
	// Note: This is conceptual - actual implementation depends on go-simple-eventstore
	src := createEventSource(db)

	// Load starting cursor from our checkpoint table
	ctx := context.Background()
	cursor, err := loadCursor(ctx, db)
	if err != nil {
		log.Fatalf("Failed to load cursor: %v", err)
	}

	log.Printf("Starting projection from cursor: %s", cursor)

	// Create and configure the runner
	runner := &projector.Runner{
		Source:    src,
		Start:     cursor,
		BatchSize: 100,
		IdleSleep: 1 * time.Second,
		Apply:     createApplyFunc(db),
		Logger: func(msg string, kv ...any) {
			log.Printf("[RUNNER] %s %v", msg, kv)
		},
	}

	// Run the projector
	log.Println("Starting projector...")
	if err := runner.Run(ctx); err != nil {
		log.Fatalf("Projector failed: %v", err)
	}
}

// createTables sets up our projection and checkpoint tables
func createTables(ctx context.Context, db *sql.DB) error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS cards_latest (
			card_id TEXT PRIMARY KEY,
			user_id TEXT NOT NULL,
			title TEXT NOT NULL,
			updated_at TIMESTAMP NOT NULL DEFAULT NOW()
		)`,
		`CREATE TABLE IF NOT EXISTS projection_checkpoints (
			projection_name TEXT PRIMARY KEY,
			cursor_value BYTEA NOT NULL,
			updated_at TIMESTAMP NOT NULL DEFAULT NOW()
		)`,
	}

	for _, query := range queries {
		if _, err := db.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("failed to execute query %q: %w", query, err)
		}
	}

	return nil
}

// loadCursor retrieves the last processed cursor from our checkpoint table
func loadCursor(ctx context.Context, db *sql.DB) (es.Cursor, error) {
	var cursor []byte
	err := db.QueryRowContext(ctx,
		`SELECT cursor_value FROM projection_checkpoints WHERE projection_name = $1`,
		"cards_latest",
	).Scan(&cursor)

	if err == sql.ErrNoRows {
		// No checkpoint found, start from beginning
		log.Println("No checkpoint found, starting from beginning")
		return es.Cursor(""), nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to load cursor: %w", err)
	}

	return es.Cursor(cursor), nil
}

// saveCursorTx saves the cursor within a transaction
func saveCursorTx(ctx context.Context, tx *sql.Tx, cursor es.Cursor) error {
	_, err := tx.ExecContext(ctx,
		`INSERT INTO projection_checkpoints (projection_name, cursor_value, updated_at)
		 VALUES ($1, $2, NOW())
		 ON CONFLICT (projection_name) 
		 DO UPDATE SET cursor_value = EXCLUDED.cursor_value, updated_at = NOW()`,
		"cards_latest", []byte(cursor))

	if err != nil {
		return fmt.Errorf("failed to save cursor: %w", err)
	}

	return nil
}

// createApplyFunc returns the Apply function that handles event projection
func createApplyFunc(db *sql.DB) projector.ApplyFunc {
	return func(ctx context.Context, batch []es.Envelope, next es.Cursor) error {
		// Begin transaction for atomic projection + checkpoint
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("failed to begin transaction: %w", err)
		}

		// Ensure transaction is cleaned up
		defer func() {
			if err != nil {
				_ = tx.Rollback()
			}
		}()

		// Process each event in the batch
		for _, envelope := range batch {
			if err := projectEventTx(ctx, tx, envelope); err != nil {
				return fmt.Errorf("failed to project event %s: %w", envelope.EventID, err)
			}
		}

		// Save the cursor to mark progress
		if err := saveCursorTx(ctx, tx, next); err != nil {
			return fmt.Errorf("failed to save cursor: %w", err)
		}

		// Commit transaction
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit transaction: %w", err)
		}

		log.Printf("Successfully projected %d events", len(batch))
		return nil
	}
}

// projectEventTx projects a single event within a transaction
func projectEventTx(ctx context.Context, tx *sql.Tx, envelope es.Envelope) error {
	switch envelope.Type {
	case "card.created":
		var event CardCreated
		if err := json.Unmarshal(envelope.Data, &event); err != nil {
			return fmt.Errorf("failed to unmarshal CardCreated: %w", err)
		}
		return upsertCardTx(ctx, tx, event.CardID, event.UserID, event.Title)

	case "card.updated":
		var event CardUpdated
		if err := json.Unmarshal(envelope.Data, &event); err != nil {
			return fmt.Errorf("failed to unmarshal CardUpdated: %w", err)
		}
		return updateCardTitleTx(ctx, tx, event.CardID, event.Title)

	default:
		// Unknown event type - skip (be lenient)
		log.Printf("Skipping unknown event type: %s", envelope.Type)
		return nil
	}
}

// upsertCardTx creates or updates a card record
func upsertCardTx(ctx context.Context, tx *sql.Tx, cardID, userID, title string) error {
	_, err := tx.ExecContext(ctx,
		`INSERT INTO cards_latest (card_id, user_id, title, updated_at)
		 VALUES ($1, $2, $3, NOW())
		 ON CONFLICT (card_id)
		 DO UPDATE SET title = EXCLUDED.title, updated_at = NOW()`,
		cardID, userID, title)

	if err != nil {
		return fmt.Errorf("failed to upsert card: %w", err)
	}

	log.Printf("Upserted card: %s", cardID)
	return nil
}

// updateCardTitleTx updates just the title of an existing card
func updateCardTitleTx(ctx context.Context, tx *sql.Tx, cardID, title string) error {
	result, err := tx.ExecContext(ctx,
		`UPDATE cards_latest SET title = $1, updated_at = NOW() WHERE card_id = $2`,
		title, cardID)

	if err != nil {
		return fmt.Errorf("failed to update card title: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		log.Printf("Warning: No card found with ID %s for title update", cardID)
	} else {
		log.Printf("Updated card title: %s", cardID)
	}

	return nil
}

// createEventSource creates a mock event source for demonstration
// In real code, this would use the actual go-simple-eventstore PostgreSQL consumer
func createEventSource(db *sql.DB) es.Consumer {
	return &mockConsumer{db: db}
}

// mockConsumer implements es.Consumer for demonstration purposes
type mockConsumer struct {
	db     *sql.DB
	events []es.Envelope
	cursor int
}

func (m *mockConsumer) Fetch(ctx context.Context, cursor es.Cursor, limit int) ([]es.Envelope, es.Cursor, error) {
	// In real implementation, this would query the events table
	// For demo, return empty batches to show idle behavior
	log.Printf("Mock consumer fetch called with cursor=%s, limit=%d", cursor, limit)
	
	// Simulate no new events
	return []es.Envelope{}, cursor, nil
}

func (m *mockConsumer) Commit(ctx context.Context, cursor es.Cursor) error {
	// For PostgreSQL, this is typically a no-op since events are already persisted
	log.Printf("Mock consumer commit called with cursor=%s", cursor)
	return nil
}