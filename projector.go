package projector

import (
	"context"
	"time"

	es "github.com/shogotsuneto/go-simple-eventstore"
)

// ApplyFunc is implemented by users to project events AND persist the 'next' cursor.
// Apply must be idempotent; return an error to have the runner stop/retry.
type ApplyFunc func(ctx context.Context, batch []es.Envelope, next es.Cursor) error

// Runner repeatedly pulls events from an event source and invokes user-provided projection logic.
// Users fully own where/how they store the checkpoint (cursor) and whether to make
// projection + checkpoint atomic (e.g., a DB transaction).
type Runner struct {
	Source     es.Consumer   // event source (Postgres, DynamoDB Streams, Kafka…)
	Apply      ApplyFunc     // user projection + checkpoint
	Start      es.Cursor     // starting cursor (user loads from their store)
	BatchSize  int           // default: 512
	IdleSleep  time.Duration // default: 200ms between empty polls
	MaxBatches int           // 0 = unlimited (useful for tests/cron)
	Logger     func(msg string, kv ...any) // optional, nil-safe
}

// Run pulls events and calls Apply with 'next' cursor after each batch.
// Flow: Fetch -> Apply (user persists data+cursor) -> Commit (source) -> advance.
func (r *Runner) Run(ctx context.Context) error {
	// Set defaults
	batchSize := r.BatchSize
	if batchSize <= 0 {
		batchSize = 512
	}
	
	idleSleep := r.IdleSleep
	if idleSleep <= 0 {
		idleSleep = 200 * time.Millisecond
	}

	cursor := r.Start
	batchCount := 0

	r.logf("runner starting", "batchSize", batchSize, "idleSleep", idleSleep, "maxBatches", r.MaxBatches)

	for {
		// Check context cancellation
		select {
		case <-ctx.Done():
			r.logf("runner stopped due to context cancellation")
			return ctx.Err()
		default:
		}

		// Check MaxBatches limit
		if r.MaxBatches > 0 && batchCount >= r.MaxBatches {
			r.logf("runner stopped after reaching MaxBatches", "maxBatches", r.MaxBatches, "processed", batchCount)
			return nil
		}

		// Fetch batch from source
		batch, next, err := r.Source.Fetch(ctx, cursor, batchSize)
		if err != nil {
			r.logf("fetch error", "error", err)
			return err
		}

		// If no events, sleep and continue
		if len(batch) == 0 {
			r.logf("no events fetched, sleeping", "idleSleep", idleSleep)
			
			select {
			case <-ctx.Done():
				r.logf("runner stopped due to context cancellation during idle sleep")
				return ctx.Err()
			case <-time.After(idleSleep):
				// Continue to next iteration
			}
			continue
		}

		r.logf("fetched batch", "eventCount", len(batch))

		// Apply user projection logic with next cursor
		err = r.Apply(ctx, batch, next)
		if err != nil {
			r.logf("apply error", "error", err, "eventCount", len(batch))
			return err
		}

		r.logf("applied batch successfully", "eventCount", len(batch))

		// Commit to source (may be no-op for some sources)
		err = r.Source.Commit(ctx, next)
		if err != nil {
			r.logf("commit error", "error", err)
			return err
		}

		// Advance cursor and increment batch count
		cursor = next
		batchCount++

		r.logf("batch processed", "batchCount", batchCount, "cursorAdvanced", true)
	}
}

// logf is a nil-safe logging helper
func (r *Runner) logf(msg string, kv ...any) {
	if r.Logger != nil {
		r.Logger(msg, kv...)
	}
}