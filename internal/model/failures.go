package model

type Status string

const (
	StatusReady   Status = "READY"
	StatusInvalid Status = "INVALID"
)

const (
	ErrInvalidEnvelope      = "invalid envelope: "
	ErrInvalidTrailingJSON  = "invalid trailing JSON"
	ErrMultipleJSONValues   = "multiple JSON values"
	ErrMissingProducerID    = "missing producer id"
	ErrMissingReceiverID    = "missing receiver id"
	ErrMissingEventID       = "missing event id"
	ErrMissingEventType     = "missing event type"
	ErrMissingSchemaVersion = "missing schema version"
	ErrMissingTenantID      = "missing tenant id"
	ErrInvalidOccurredAt    = "occured_at must be non zero and no more than +24 hours"
	ErrMissingPayload       = "missing payload"
	ErrUnknownEventContract = "unknown event contract: "
	ErrInvalidPayloadJSON   = "invalid payload JSON: "
	ErrSchemaValidation     = "schema validation: "
	ErrNoAuthorizedRoute    = "no authorized router for tenant event type receiver id"
)
