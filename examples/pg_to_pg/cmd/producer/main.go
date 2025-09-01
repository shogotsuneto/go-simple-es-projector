// Package main demonstrates adding events to the event store.
// This shows how to produce events that the projector example consumes.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/lib/pq" // PostgreSQL driver
	es "github.com/shogotsuneto/go-simple-eventstore"
	"github.com/shogotsuneto/go-simple-eventstore/postgres"
)

// Example event types - duplicated here for simplicity
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
	// Get event store connection
	eventstoreURL := os.Getenv("EVENTSTORE_URL")
	if eventstoreURL == "" {
		eventstoreURL = "postgres://eventstore_user:eventstore_pass@localhost:5432/eventstore?sslmode=disable"
		log.Printf("Using default EVENTSTORE_URL: %s", eventstoreURL)
	}

	// Create event store
	config := postgres.Config{
		ConnectionString: eventstoreURL,
		TableName:        "events",
	}

	eventStore, err := postgres.NewPostgresEventStore(config)
	if err != nil {
		log.Fatalf("Failed to create event store: %v", err)
	}

	ctx := context.Background()

	// Add some new events
	log.Println("Adding new product tag events...")

	// Add events for a new product
	if err := addTagEvents(ctx, eventStore); err != nil {
		log.Fatalf("Failed to add events: %v", err)
	}

	log.Println("Events added successfully!")
	log.Println("Run the projector (go run main.go) to see these events projected.")
}

func addTagEvents(ctx context.Context, eventStore es.EventStore) error {
	productID := "product-999"
	streamID := productID
	userID := "user-demo"

	// Add multiple tags to demonstrate batch processing
	events := []es.Event{
		{
			Type: "product.tag_added",
			Data: mustMarshal(TagAdded{
				ProductID: productID,
				Tag:       "gadgets",
				UserID:    userID,
			}),
			Metadata: map[string]string{
				"source":    "producer-demo",
				"timestamp": time.Now().Format(time.RFC3339),
			},
		},
		{
			Type: "product.tag_added",
			Data: mustMarshal(TagAdded{
				ProductID: productID,
				Tag:       "wireless",
				UserID:    userID,
			}),
			Metadata: map[string]string{
				"source":    "producer-demo",
				"timestamp": time.Now().Format(time.RFC3339),
			},
		},
		{
			Type: "product.tag_added",
			Data: mustMarshal(TagAdded{
				ProductID: productID,
				Tag:       "bluetooth",
				UserID:    userID,
			}),
			Metadata: map[string]string{
				"source":    "producer-demo",
				"timestamp": time.Now().Format(time.RFC3339),
			},
		},
	}

	// Append events to stream (-1 means any version, no concurrency check)
	_, err := eventStore.Append(streamID, events, -1)
	if err != nil {
		return fmt.Errorf("failed to append events to stream %s: %w", streamID, err)
	}

	log.Printf("Added %d events to stream: %s", len(events), streamID)

	// Add a removal event to another stream to demonstrate that too
	removalEvents := []es.Event{
		{
			Type: "product.tag_removed",
			Data: mustMarshal(TagRemoved{
				ProductID: "product-123", // Remove from existing product
				Tag:       "electronics",
				UserID:    userID,
			}),
			Metadata: map[string]string{
				"source":    "producer-demo",
				"timestamp": time.Now().Format(time.RFC3339),
			},
		},
	}

	_, err = eventStore.Append("product-123", removalEvents, -1)
	if err != nil {
		return fmt.Errorf("failed to append removal event: %w", err)
	}

	log.Printf("Added tag removal event to stream: product-123")

	return nil
}

func mustMarshal(v interface{}) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("failed to marshal: %v", err))
	}
	return data
}
