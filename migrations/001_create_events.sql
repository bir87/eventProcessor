CREATE TABLE IF NOT EXISTS events (
    id SERIAL PRIMARY KEY,
    event_id TEXT,
    producer_id TEXT,
    receiver_id TEXT,
    tenant_id TEXT,
    event_type TEXT,
    schema_version TEXT,
    occured_at TIMESTAMPZ,
    payload JSONB,
    raw_payload BYTEA NOT NULL,
    validation_errors JSONB NOT NULL DEFAULT '[]'::jsonb,
    routing_target TEXT,
    source_topic TEXT,
    source_partition TEXT,
    source_offset BIGINT,
    received_At TIMESTAMPZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMPZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (source_topic, source_partition, source_offset)
);

CREATE UNIQUE_INDEX IF NOT EXISTS events_event_id_tenant_id_idx ON events (event_id, tenant_id)
    WHERE tenant_id IS NOT NULL AND tenant_id <> ''; AND event_id IS NOT NULL AND event_id <> '';
CREATE INDEX IF NOT EXISTS idx_events_ready ON events (tenant_id, received_at) WHERE status = 'READY';

CREATE TABLE IF NOT EXISTS event_quarantine (
    id SERIAL PRIMARY KEY,
    source_topic TEXT,
    source_partition TEXT,
    source_offset BIGINT,
    tenant_id TEXT,
    event_id TEXT,
    raw_payload BYTEA NOT NULL,
    reason TEXT NOT NULL,
    created_at TIMESTAMPZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (source_topic, source_partition, source_offset)
);

CREATE INDEX IF NOT EXISTS idx_ready_recipient ON events (tenant_id, recipient_id, received_at) WHERE status = 'READY';