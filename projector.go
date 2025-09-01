package projector

import (
	"context"
	"time"

	es "github.com/shogotsuneto/go-simple-eventstore"
)

// ApplyFunc is implemented by users to project events AND persist the 'next' cursor.
// Apply must be idempotent; return an error to have the worker stop/retry.
type ApplyFunc func(ctx context.Context, batch []es.Envelope, next es.Cursor) error

// Worker repeatedly pulls events from an event source and invokes user-provided projection logic.
// Users fully own where/how they store the checkpoint (cursor) and whether to make
// projection + checkpoint atomic (e.g., a DB transaction).
type Worker struct {
	Source     es.Consumer   // event source (Postgres, DynamoDB Streams, Kafka…)
	Apply      ApplyFunc     // user projection + checkpoint
	Start      es.Cursor     // starting cursor (user loads from their store)
	BatchSize  int           // default: 512
	IdleSleep  time.Duration // default: 200ms between empty polls
	Logger     func(msg string, kv ...any) // optional, nil-safe
}

// Run pulls events and calls Apply with 'next' cursor after each batch.
// Flow: Fetch -> Apply (user persists data+cursor) -> Commit (source) -> advance.
func (w *Worker) Run(ctx context.Context) error {
	// Set defaults
	batchSize := w.BatchSize
	if batchSize <= 0 {
		batchSize = 512
	}
	
	idleSleep := w.IdleSleep
	if idleSleep <= 0 {
		idleSleep = 200 * time.Millisecond
	}

	cursor := w.Start
	
	w.logf("worker starting", "batchSize", batchSize, "idleSleep", idleSleep)

	for {
		// Check context cancellation
		select {
		case <-ctx.Done():
			w.logf("worker stopped due to context cancellation")
			return ctx.Err()
		default:
		}

		// Fetch batch from source
		batch, next, err := w.Source.Fetch(ctx, cursor, batchSize)
		if err != nil {
			w.logf("fetch error", "error", err)
			return err
		}

		// If no events, sleep and continue
		if len(batch) == 0 {
			w.logf("no events fetched, sleeping", "idleSleep", idleSleep)
			
			select {
			case <-ctx.Done():
				w.logf("worker stopped due to context cancellation during idle sleep")
				return ctx.Err()
			case <-time.After(idleSleep):
				// Continue to next iteration
			}
			continue
		}

		w.logf("fetched batch", "eventCount", len(batch))

		// Apply user projection logic with next cursor
		err = w.Apply(ctx, batch, next)
		if err != nil {
			w.logf("apply error", "error", err, "eventCount", len(batch))
			return err
		}

		w.logf("applied batch successfully", "eventCount", len(batch))

		// Commit to source (may be no-op for some sources)
		err = w.Source.Commit(ctx, next)
		if err != nil {
			w.logf("commit error", "error", err)
			return err
		}

		// Advance cursor
		cursor = next

		w.logf("batch processed", "cursorAdvanced", true)
	}
}

// logf is a nil-safe logging helper
func (w *Worker) logf(msg string, kv ...any) {
	if w.Logger != nil {
		w.Logger(msg, kv...)
	}
}