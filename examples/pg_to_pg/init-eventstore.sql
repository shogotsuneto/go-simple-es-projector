-- Initialize event store database

-- Events table for go-simple-eventstore (required schema)
CREATE TABLE IF NOT EXISTS events (
		id SERIAL PRIMARY KEY,
		stream_id VARCHAR(255) NOT NULL,
		version BIGINT NOT NULL,
		event_id VARCHAR(255) NOT NULL,
		event_type VARCHAR(255) NOT NULL,
		event_data BYTEA NOT NULL,
		metadata JSONB,
        timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_events_stream_id ON events(stream_id);
CREATE INDEX IF NOT EXISTS idx_events_stream_version ON events(stream_id, version);
CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events(timestamp);

-- Insert some sample product tag events for demonstration
INSERT INTO events (stream_id, version, event_id, event_type, event_data, metadata) VALUES
('product-123', 1, 'evt-123-1', 'product.tag_added', '{"product_id": "product-123", "tag": "electronics", "user_id": "user-1"}'::bytea, '{"source": "product-service"}'),
('product-123', 2, 'evt-123-2', 'product.tag_added', '{"product_id": "product-123", "tag": "mobile", "user_id": "user-1"}'::bytea, '{"source": "product-service"}'),
('product-456', 1, 'evt-456-1', 'product.tag_added', '{"product_id": "product-456", "tag": "books", "user_id": "user-2"}'::bytea, '{"source": "product-service"}'),
('product-456', 2, 'evt-456-2', 'product.tag_added', '{"product_id": "product-456", "tag": "fiction", "user_id": "user-2"}'::bytea, '{"source": "product-service"}'),
('product-123', 3, 'evt-123-3', 'product.tag_removed', '{"product_id": "product-123", "tag": "mobile", "user_id": "user-1"}'::bytea, '{"source": "product-service"}'),
('product-789', 1, 'evt-789-1', 'product.tag_added', '{"product_id": "product-789", "tag": "electronics", "user_id": "user-3"}'::bytea, '{"source": "product-service"}'),
('product-789', 2, 'evt-789-2', 'product.tag_added', '{"product_id": "product-789", "tag": "computers", "user_id": "user-3"}'::bytea, '{"source": "product-service"}');