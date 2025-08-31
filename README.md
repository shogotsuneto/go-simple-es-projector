# go-simple-es-projector

A minimal event runner that repeatedly pulls events from an event source and invokes **user-provided projection logic**. 

## Responsibilities

**You (the user) fully own:**
- where/how you **store the checkpoint** (cursor)
- whether to make projection + checkpoint **atomic** (e.g., a DB transaction)
- any **DB/driver choices** (`database/sql`, `pgx`, DynamoDB SDK, etc.)

**The runner is generic; users provide Apply functions and checkpoint storage.**  
**Any transactional atomicity is user-managed.**  
**Delivery is at-least-once; Apply must be idempotent.**

This package depends on `github.com/shogotsuneto/go-simple-eventstore` for the Consumer interface and event types.

## Installation

```bash
go get github.com/shogotsuneto/go-simple-es-projector
```

## API

```go
package projector

import (
    "context"
    "time"
    es "github.com/shogotsuneto/go-simple-eventstore"
)

// Users implement Apply to project events AND persist 'next' cursor.
// Apply must be idempotent; return an error to have the runner stop/retry.
type ApplyFunc func(ctx context.Context, batch []es.Envelope, next es.Cursor) error

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
func (r *Runner) Run(ctx context.Context) error
```

## Behavior

1. `cursor := Start`
2. loop:
   - `batch, next, err := Source.Fetch(ctx, cursor, BatchSize)`
   - if error → return error
   - if `len(batch)==0` → sleep `IdleSleep`, continue
   - `err := Apply(ctx, batch, next)` (user persists read model + checkpoint; can be atomic)
   - if error → return error (runner doesn't swallow apply failures)
   - `err := Source.Commit(ctx, next)` (Kafka may use this; others can no-op)
   - `cursor = next`
   - stop on `ctx.Done()` or `MaxBatches` reached

## Usage Examples

### Closure capturing `*sql.DB` (inline transaction)

```go
db  := mustOpenPostgres() // *sql.DB is goroutine-safe
src := pgconsumer.New(db, pgconsumer.WithTable("events")) // from go-simple-eventstore
cur := loadCursorFromMyTable(ctx, db) // user-defined

r := &projector.Runner{
  Source:    src,
  Start:     cur,
  BatchSize: 500,
  Apply: func(ctx context.Context, batch []es.Envelope, next es.Cursor) error {
    tx, err := db.BeginTx(ctx, nil)
    if err != nil { return err }
    for _, ev := range batch {
      if err := upsertSchedulingTx(ctx, tx, ev); err != nil { _ = tx.Rollback(); return err }
    }
    if err := saveCursorTx(ctx, tx, next); err != nil { _ = tx.Rollback(); return err }
    return tx.Commit()
  },
}
if err := r.Run(ctx); err != nil { log.Fatal(err) }
```

### Method value (receiver "binds" dependencies)

```go
type App struct {
  DB *sql.DB
}

func (a *App) Apply(ctx context.Context, batch []es.Envelope, next es.Cursor) error {
  tx, err := a.DB.BeginTx(ctx, nil)
  if err != nil { return err }
  for _, ev := range batch {
    if err := upsertCardsLatestTx(ctx, tx, ev); err != nil { _ = tx.Rollback(); return err }
  }
  if err := saveCursorTx(ctx, tx, next); err != nil { _ = tx.Rollback(); return err }
  return tx.Commit()
}

app := &App{DB: db}
r := &projector.Runner{ Source: src, Start: cur, Apply: app.Apply }
_ = r.Run(ctx)
```

### Non-SQL store (DynamoDB Streams → DynamoDB)

```go
dynamo := dynamodb.NewFromConfig(cfg)
src    := ddbstreams.NewConsumer(cfg, /* adapter opts */)
cur    := loadCursorFromDynamo(ctx, dynamo, "checkpoints-table")

r := &projector.Runner{
  Source: src,
  Start:  cur,
  Apply: func(ctx context.Context, batch []es.Envelope, next es.Cursor) error {
    // Upsert items (idempotent) then save 'next' to your checkpoints table
    if err := applyBatchToDynamo(ctx, dynamo, batch); err != nil { return err }
    return saveCursorToDynamo(ctx, dynamo, "checkpoints-table", next)
  },
}
_ = r.Run(ctx)
```

> These examples intentionally **do not** rely on any helper from this repo; users decide isolation levels, retries, and schema.

## Commit semantics

The `Commit` method on the source is called after `Apply` succeeds:
- **Kafka**: May advance consumer group offset
- **DynamoDB Streams**: Typically no-op (checkpoint is in your table)
- **Postgres**: Often no-op (events are already committed to the table)

## Key Points

- **At-least-once delivery**: Apply must be idempotent
- **User-managed atomicity**: You control transactions and consistency
- **Generic runner**: Works with any storage backend
- **Context-aware**: Respects cancellation and timeouts
- **Configurable**: Batch sizes, idle sleep, max batches for testing

## Requirements

- Go ≥ 1.21
- `github.com/shogotsuneto/go-simple-eventstore` for the Consumer interface