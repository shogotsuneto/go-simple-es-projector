-- Initialize event store database
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Events table for go-simple-eventstore
CREATE TABLE IF NOT EXISTS events (
    stream_id TEXT NOT NULL,
    version BIGINT NOT NULL,
    event_id UUID NOT NULL DEFAULT uuid_generate_v4(),
    event_type TEXT NOT NULL,
    event_data JSONB NOT NULL,
    metadata JSONB,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (stream_id, version),
    UNIQUE (event_id)
);

CREATE INDEX IF NOT EXISTS idx_events_created_at ON events(created_at);
CREATE INDEX IF NOT EXISTS idx_events_event_type ON events(event_type);
CREATE INDEX IF NOT EXISTS idx_events_event_id ON events(event_id);

-- Insert some sample product tag events for demonstration
INSERT INTO events (stream_id, version, event_type, event_data, metadata) VALUES
('product-123', 1, 'product.tag_added', '{"product_id": "product-123", "tag": "electronics", "user_id": "user-1"}', '{"source": "product-service"}'),
('product-123', 2, 'product.tag_added', '{"product_id": "product-123", "tag": "mobile", "user_id": "user-1"}', '{"source": "product-service"}'),
('product-456', 1, 'product.tag_added', '{"product_id": "product-456", "tag": "books", "user_id": "user-2"}', '{"source": "product-service"}'),
('product-456', 2, 'product.tag_added', '{"product_id": "product-456", "tag": "fiction", "user_id": "user-2"}', '{"source": "product-service"}'),
('product-123', 3, 'product.tag_removed', '{"product_id": "product-123", "tag": "mobile", "user_id": "user-1"}', '{"source": "product-service"}'),
('product-789', 1, 'product.tag_added', '{"product_id": "product-789", "tag": "electronics", "user_id": "user-3"}', '{"source": "product-service"}'),
('product-789', 2, 'product.tag_added', '{"product_id": "product-789", "tag": "computers", "user_id": "user-3"}', '{"source": "product-service"}');