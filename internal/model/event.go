package model

import (
	"encoding/json"
	"time"
)

type Envelope struct {
	ProducerID    string          `json:"producer_id"`
	ReceiverID    string          `json:"receiver_id"`
	EventID       string          `json:"event_id"`
	EventType     string          `json:"event_type"`
	SchemaVersion string          `json:"schema_version"`
	TenantID      string          `json:"tenant_id"`
	OccuredAt     time.Time       `json:"occured_at"`
	Payload       json.RawMessage `json:"payload"`
}

type Source struct {
	Topic     string
	Partition int
	Offset    int64
}

type Record struct {
	Envelope Envelope
	Raw      []byte
	Source   Source
	Status   string
	Errors   []string
	Route    string
}
