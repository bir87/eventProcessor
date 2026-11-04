package validation

import (
	"eventprocessor/internal/model"
	"os"
	"strings"
	"testing"
)

func mustValidator(t *testing.T) *Validator {
	t.Helper()
	v, err := New(os.DirFS("../../schemas"))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return v
}

func hasError(r model.Record, substr string) bool {
	for _, e := range r.Errors {
		if strings.Contains(e, substr) {
			return true
		}
	}
	return false
}

func TestValidate_ValidEventIsReady(t *testing.T) {
	v := mustValidator(t)
	raw := []byte(`{
		"producer_id": "transaction.authorized",
		"receiver_id": "banking-app",
		"event_id": "evt-1",
		"event_type": "transaction.authorized",
		"schema_version": "v1",
		"tenant_id": "tenant-demo",
		"occured_at": "2026-01-01T00:00:00Z",
		"payload":{"transaction_id":"tx-1", "amount":10.5, "currency":"USD"}
	}`)

	r := v.Validate(raw, model.Source{Topic: "t", Partition: 0, Offset: 1})

	if r.Status != string(model.StatusReady) {
		t.Fatalf("Status = %q, errors = %v; want READY with no errors", r.Status, r.Errors)
	}

	if len(r.Errors) != 0 {
		t.Fatalf("Errors= %v: should be none", r.Errors)
	}

	if r.Envelope.EventID != "evt-1" {
		t.Fatalf("Envelope.EventID= %q: want evt-1", r.Envelope.EventID)
	}
}

func TestValidate_MissingRequiredFields(t *testing.T) {
	v := mustValidator(t)
	raw := []byte(`{"payload": {"transaction_id" : "tx-1", "amount": 1, "currency":"USD"}}`)

	r := v.Validate(raw, model.Source{})

	if r.Status != string(model.StatusInvalid) {
		t.Fatalf("Status = %q; want INVALID", r.Status)
	}

	for _, want := range []string{
		model.ErrMissingProducerID, model.ErrMissingReceiverID, model.ErrMissingEventID,
		model.ErrMissingEventType, model.ErrMissingSchemaVersion, model.ErrMissingTenantID,
		model.ErrInvalidOccurredAt,
	} {
		if !hasError(r, want) {
			t.Errorf("missing expected error %q in %v", want, r.Errors)
		}
	}
}
