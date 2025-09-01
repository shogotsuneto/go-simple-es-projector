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
	"strconv"
	"time"

	"github.com/shogotsuneto/go-simple-es-projector"
	es "github.com/shogotsuneto/go-simple-eventstore"
	"github.com/shogotsuneto/go-simple-eventstore/postgres"
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
	// Environment variables for database connections
	eventstoreURL := os.Getenv("EVENTSTORE_URL")
	if eventstoreURL == "" {
		eventstoreURL = "postgres://eventstore_user:eventstore_pass@localhost:5432/eventstore?sslmode=disable"
		log.Printf("Using default EVENTSTORE_URL: %s", eventstoreURL)
	}

	projectionURL := os.Getenv("PROJECTION_URL")
	if projectionURL == "" {
		projectionURL = "postgres://projection_user:projection_pass@localhost:5433/projections?sslmode=disable"
		log.Printf("Using default PROJECTION_URL: %s", projectionURL)
	}

	// Check for timeout environment variable
	var timeout time.Duration
	if timeoutEnv := os.Getenv("PROJECTOR_TIMEOUT"); timeoutEnv != "" {
		timeoutSeconds, err := strconv.Atoi(timeoutEnv)
		if err != nil {
			log.Fatalf("Invalid PROJECTOR_TIMEOUT value '%s': must be a number of seconds", timeoutEnv)
		}
		timeout = time.Duration(timeoutSeconds) * time.Second
		log.Printf("Projector will run for %v", timeout)
	}

	// Connect to projection database
	projectionDB, err := sql.Open("postgres", projectionURL)
	if err != nil {
		log.Fatalf("Failed to open projection database: %v", err)
	}
	defer projectionDB.Close()

	// Verify projection database connection
	if err := projectionDB.Ping(); err != nil {
		log.Printf("Projection database connection failed: %v", err)
		log.Printf("Skipping example - requires PostgreSQL. Run: docker-compose up -d")
		return
	}

	// Create projection tables (in real code, use migrations)
	if err := createProjectionTables(context.Background(), projectionDB); err != nil {
		log.Fatalf("Failed to create projection tables: %v", err)
	}

	// Create event source using go-simple-eventstore
	src, err := createEventConsumer(eventstoreURL)
	if err != nil {
		log.Fatalf("Failed to create event consumer: %v", err)
	}

	// Load starting cursor from our checkpoint table
	ctx := context.Background()
	
	// Apply timeout if specified
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	
	cursor, err := loadCursor(ctx, projectionDB)
	if err != nil {
		log.Fatalf("Failed to load cursor: %v", err)
	}

	log.Printf("Starting projection from cursor: %s", cursor)

	// Create and configure the worker
	worker := &projector.Worker{
		Source:    src,
		Start:     cursor,
		BatchSize: 10, // Small batches for demo
		IdleSleep: 2 * time.Second,
		Apply:     createApplyFunc(projectionDB),
		Logger: func(msg string, kv ...any) {
			log.Printf("[WORKER] %s %v", msg, kv)
		},
	}

	// Run the projector
	if timeout > 0 {
		log.Printf("Starting projector with %v timeout...", timeout)
	} else {
		log.Println("Starting projector...")
	}
	
	err = worker.Run(ctx)
	if err == context.DeadlineExceeded {
		log.Printf("Projector stopped after %v timeout", timeout)
	} else if err != nil {
		log.Fatalf("Projector failed: %v", err)
	} else {
		log.Println("Projection completed successfully!")
	}
}

// createProjectionTables sets up our projection and checkpoint tables
func createProjectionTables(ctx context.Context, db *sql.DB) error {
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

// createEventConsumer creates a real event consumer using go-simple-eventstore
func createEventConsumer(eventstoreURL string) (es.Consumer, error) {
	config := postgres.Config{
		ConnectionString: eventstoreURL,
		TableName:        "events",
	}

	consumer, err := postgres.NewPostgresEventConsumer(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create postgres event consumer: %w", err)
	}

	return consumer, nil
}

