# PostgreSQL to PostgreSQL Projection Example

This example demonstrates a complete event sourcing projection using `go-simple-es-projector` with real `go-simple-eventstore` integration. It showcases projecting product tag events from an event store to a read model optimized for tag-based product searches.

## Architecture

The example uses **two separate PostgreSQL databases**:

1. **Event Store Database** (`eventstore-db:5432`): Contains the source events using `go-simple-eventstore`
2. **Projection Database** (`projection-db:5433`): Contains the projected read model and checkpoints

This separation demonstrates best practices for CQRS/Event Sourcing where the event store and read models are isolated.

## Features Demonstrated

- ✅ Real `go-simple-eventstore` integration (not mocked)
- ✅ Separate databases for event store vs projections
- ✅ Docker Compose setup with PostgreSQL
- ✅ Atomic projection + checkpoint persistence
- ✅ Idempotent event handling (`ON CONFLICT DO NOTHING`)
- ✅ Sample events pre-loaded for immediate demonstration
- ✅ Tag-based product search optimization

## Event Types

The example processes these product events:

```go
// Add a tag to a product
type TagAdded struct {
    ProductID string `json:"product_id"`
    Tag       string `json:"tag"`
    UserID    string `json:"user_id"`
}

// Remove a tag from a product  
type TagRemoved struct {
    ProductID string `json:"product_id"`
    Tag       string `json:"tag"`
    UserID    string `json:"user_id"`
}
```

## Projection Schema

Events are projected to a `product_tags` table optimized for tag searches:

```sql
CREATE TABLE product_tags (
    product_id TEXT NOT NULL,
    tag TEXT NOT NULL,
    added_by TEXT NOT NULL,
    added_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (product_id, tag)
);

CREATE INDEX idx_product_tags_tag ON product_tags(tag);        -- Search by tag
CREATE INDEX idx_product_tags_product_id ON product_tags(product_id); -- Search by product
```

## Quick Start

### Using Makefile (Recommended)

```bash
# Start databases and run full demo
make demo

# Or step by step:
make up          # Start databases  
make run         # Run projector
make producer    # Add more events
make run         # Run projector again
make down        # Stop databases
```

### Manual Steps

### 1. Start the databases

```bash
docker-compose up -d
```

This creates:
- Event store at `localhost:5432` with sample events
- Projection database at `localhost:5433` 

### 2. Run the projector

```bash
go run main.go
```

The projector will:
1. Connect to both databases
2. Load checkpoint (starts from beginning on first run)
3. Fetch events from the event store
4. Project them to the `product_tags` table
5. Save checkpoint atomically
6. Display results

### 3. (Optional) Add more events

```bash
# Using the Makefile
make producer

# Or manually
cd cmd/producer && go run main.go
```

This adds new events to the event store that you can then project by running the main projector again.

### 4. View the results

The example shows:
- All projected product tags
- Current checkpoint position
- Example tag-based queries

```
=== PROJECTION RESULTS ===
Product Tags:
  product-123 -> electronics (by user-1 at 14:30:15)
  product-456 -> books (by user-2 at 14:30:15)
  product-456 -> fiction (by user-2 at 14:30:15)
  product-789 -> computers (by user-3 at 14:30:15)
  product-789 -> electronics (by user-3 at 14:30:15)

Checkpoint: 7

Example tag-based searches:
  Products with 'electronics' tag: [product-123 product-789]
```

## Files

- `main.go` - The projector that reads events and creates the read model
- `cmd/producer/main.go` - Example event producer to add new events to the event store
- `docker-compose.yml` - PostgreSQL databases setup
- `init-eventstore.sql` - Event store schema and sample data
- `init-projections.sql` - Projection database schema
- `Makefile` - Convenient commands for running the example

## Sample Data

The event store comes pre-loaded with sample events:

| Product | Event | Tag | Result |
|---------|-------|-----|--------|
| product-123 | tag_added | electronics | ✅ Added |
| product-123 | tag_added | mobile | ✅ Added |
| product-456 | tag_added | books | ✅ Added |
| product-456 | tag_added | fiction | ✅ Added |
| product-123 | tag_removed | mobile | ❌ Removed |
| product-789 | tag_added | electronics | ✅ Added |
| product-789 | tag_added | computers | ✅ Added |

## Configuration

Environment variables (with defaults):

```bash
# Event store database (source)
EVENTSTORE_URL="postgres://eventstore_user:eventstore_pass@localhost:5432/eventstore?sslmode=disable"

# Projection database (target)
PROJECTION_URL="postgres://projection_user:projection_pass@localhost:5433/projections?sslmode=disable"
```

## Key Implementation Details

### Atomic Transactions
Each batch is processed in a single transaction that includes both the projection updates AND checkpoint saving:

```go
tx, err := db.BeginTx(ctx, nil)
// 1. Project events to product_tags
// 2. Save cursor to projection_checkpoints  
// 3. Commit (or rollback on any error)
```

### Idempotent Operations
Tag additions use `ON CONFLICT DO NOTHING` to handle duplicate events safely:

```sql
INSERT INTO product_tags (product_id, tag, added_by, added_at)
VALUES ($1, $2, $3, NOW())
ON CONFLICT (product_id, tag) DO NOTHING
```

### Cursor Management
The projector tracks progress using cursors stored in the projection database:

```sql
-- Load last checkpoint
SELECT cursor_value FROM projection_checkpoints WHERE projection_name = 'product_tags'

-- Save new checkpoint (in same transaction as projections)
INSERT INTO projection_checkpoints (projection_name, cursor_value) VALUES (...)
ON CONFLICT (projection_name) DO UPDATE SET cursor_value = EXCLUDED.cursor_value
```

## Cleanup

```bash
docker-compose down -v  # Remove containers and volumes
```

## Real-World Usage

This example demonstrates patterns suitable for production:

1. **Separate Databases**: Event store and read models are isolated
2. **Atomic Operations**: Projections and checkpoints are transactionally consistent  
3. **Idempotent Handling**: Safe replay of events
4. **Configurable**: Environment-based configuration
5. **Observable**: Structured logging of projection progress

Adapt the event types, schema, and Apply function for your specific domain events and read model requirements.