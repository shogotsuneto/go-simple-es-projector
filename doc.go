// Package projector provides a minimal event worker that repeatedly pulls events
// from an event source and invokes user-provided projection logic.
//
// Users fully own:
//   - where/how they store the checkpoint (cursor)
//   - whether to make projection + checkpoint atomic (e.g., a DB transaction)
//   - any DB/driver choices (database/sql, pgx, DynamoDB SDK, etc.)
//
// The Worker is generic; users provide Apply functions and checkpoint storage.
// Any transactional atomicity is user-managed.
// Delivery is at-least-once; Apply must be idempotent.
//
// This package depends on github.com/shogotsuneto/go-simple-eventstore for
// the Consumer interface and event types.
package projector
