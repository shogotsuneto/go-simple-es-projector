-- Initialize projection database
-- This will contain our projected data and checkpoints

-- Product tags projection table
CREATE TABLE IF NOT EXISTS product_tags (
    product_id TEXT NOT NULL,
    tag TEXT NOT NULL,
    added_by TEXT NOT NULL,
    added_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (product_id, tag)
);

-- Indexes for efficient tag-based product searches
CREATE INDEX IF NOT EXISTS idx_product_tags_tag ON product_tags(tag);
CREATE INDEX IF NOT EXISTS idx_product_tags_product_id ON product_tags(product_id);

-- Checkpoint table for tracking projection progress
CREATE TABLE IF NOT EXISTS projection_checkpoints (
    projection_name TEXT PRIMARY KEY,
    cursor_value BYTEA NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);