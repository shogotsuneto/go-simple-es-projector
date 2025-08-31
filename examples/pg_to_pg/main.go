// Package main demonstrates a complete PostgreSQL-to-PostgreSQL projection example
// using database/sql with inline transactions.
//
// This example shows:
// - Loading a starting cursor from user-managed storage
// - Running the projector with user-defined Apply function
// - Atomic projection + checkpoint persistence using database transactions
// - Restarting from the saved cursor without re-applying past events
// - Projecting product tag events to enable product search by tags
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
type TagAdded struct {
	ProductID string `json:"product_id"`
	Tag       string `json:"tag"`
	UserID    string `json:"user_id"`
}

type TagRemoved struct {
	ProductID string `json:"product_id"`
	Tag       string `json:"tag"`
	UserID    string `json:"user_id"`
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
		`CREATE TABLE IF NOT EXISTS product_tags (
			product_id TEXT NOT NULL,
			tag TEXT NOT NULL,
			added_by TEXT NOT NULL,
			added_at TIMESTAMP NOT NULL DEFAULT NOW(),
			PRIMARY KEY (product_id, tag)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_product_tags_tag ON product_tags(tag)`,
		`CREATE INDEX IF NOT EXISTS idx_product_tags_product_id ON product_tags(product_id)`,
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
		"product_tags",
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
		"product_tags", []byte(cursor))

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
	case "product.tag_added":
		var event TagAdded
		if err := json.Unmarshal(envelope.Data, &event); err != nil {
			return fmt.Errorf("failed to unmarshal TagAdded: %w", err)
		}
		return addProductTagTx(ctx, tx, event.ProductID, event.Tag, event.UserID)

	case "product.tag_removed":
		var event TagRemoved
		if err := json.Unmarshal(envelope.Data, &event); err != nil {
			return fmt.Errorf("failed to unmarshal TagRemoved: %w", err)
		}
		return removeProductTagTx(ctx, tx, event.ProductID, event.Tag)

	default:
		// Unknown event type - skip (be lenient)
		log.Printf("Skipping unknown event type: %s", envelope.Type)
		return nil
	}
}

// addProductTagTx adds a tag to a product
func addProductTagTx(ctx context.Context, tx *sql.Tx, productID, tag, userID string) error {
	_, err := tx.ExecContext(ctx,
		`INSERT INTO product_tags (product_id, tag, added_by, added_at)
		 VALUES ($1, $2, $3, NOW())
		 ON CONFLICT (product_id, tag) DO NOTHING`,
		productID, tag, userID)

	if err != nil {
		return fmt.Errorf("failed to add product tag: %w", err)
	}

	log.Printf("Added tag '%s' to product: %s", tag, productID)
	return nil
}

// removeProductTagTx removes a tag from a product
func removeProductTagTx(ctx context.Context, tx *sql.Tx, productID, tag string) error {
	result, err := tx.ExecContext(ctx,
		`DELETE FROM product_tags WHERE product_id = $1 AND tag = $2`,
		productID, tag)

	if err != nil {
		return fmt.Errorf("failed to remove product tag: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		log.Printf("Warning: No tag '%s' found for product %s to remove", tag, productID)
	} else {
		log.Printf("Removed tag '%s' from product: %s", tag, productID)
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