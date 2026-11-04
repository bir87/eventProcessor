CREATE TABLE IF NOT EXISTS events (
    id SERIAL PRIMARY KEY,
    event_id TEXT,
    producer_id TEXT,
    receiver_id TEXT,
    tenant_id TEXT,
    event_type TEXT,
    schema_version TEXT,
    occured_at TIMESTAMPTZ,
    payload JSONB,
    raw_payload BYTEA NOT NULL,
    validation_errors JSONB NOT NULL DEFAULT '[]'::jsonb,
    status TEXT NOT NULL DEFAULT 'INVALID',
    routing_target TEXT,
    source_topic TEXT,
    source_partition INT,
    source_offset BIGINT,
    received_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (source_topic, source_partition, source_offset),
    UNIQUE (tenant_id, event_id)
);

CREATE INDEX IF NOT EXISTS idx_events_ready ON events (tenant_id, received_at) WHERE status = 'Ready';
CREATE INDEX IF NOT EXISTS idx_events_ready_receiver ON events (tenant_id, receiver_id, received_at) WHERE status = 'Ready';


CREATE TABLE IF NOT EXISTS event_quarantine (
    id SERIAL PRIMARY KEY,
    source_topic TEXT,
    source_partition INT,
    source_offset BIGINT,
    tenant_id TEXT,
    event_id TEXT,
    raw_payload BYTEA NOT NULL,
    reason TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (source_topic, source_partition, source_offset)
);