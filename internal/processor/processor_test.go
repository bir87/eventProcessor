package processor

import (
	"context"
	"eventprocessor/internal/model"
	"eventprocessor/internal/routing"
	"eventprocessor/internal/validation"
	"os"
	"testing"
)

type fakeStore struct {
	saved       []model.Record
	quarantined bool
	err         error
}

func (f *fakeStore) Save(_ context.Context, r model.Record) (bool, error) {
	f.saved = append(f.saved, r)
	if f.err != nil {
		return false, f.err
	}
	return f.quarantined, nil
}

func mustValidator(t *testing.T) *validation.Validator {
	t.Helper()
	v, err := validation.New(os.DirFS("../../schemas"))
	if err != nil {
		t.Fatalf("validaion.New() error = %v", err)
	}
	return v
}

func mustRouter(t *testing.T) *routing.Router {
	t.Helper()
	r, err := routing.New(os.DirFS("../../config"), "routes.json")
	if err != nil {
		t.Fatalf("routing.New() error = %v", err)
	}
	return r
}

func validTransactionEvent() []byte {
	return []byte(`{
		"producer_id": "transaction-authorizer",
		"receiver_id": "banking-app",
		"event_id": "evt-1",
		"event_type":"transaction.authorized",
		"schema_version":"v1",
		"tenant_id":"tenant-demo",
		"occured_at":"2026-01-01T00:00:00Z",
		"payload": {"transaction_id":"tx-1", "amount":10, "currency":"USD"}
	}`)
}

func TestProcess_ValidEventIsRoutedAndSaved(t *testing.T) {
	store := &fakeStore{}
	p := &Processor{
		Validator: mustValidator(t),
		Router:    mustRouter(t),
		Store:     store,
	}

	rec, quarantined, err := p.Process(context.Background(),
		validTransactionEvent(),
		model.Source{
			Topic:  "t",
			Offset: 1,
		})

	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	if quarantined {
		t.Fatalf("quarantined true; want false")
	}

	if rec.Status != string(model.StatusReady) {
		t.Fatalf("Status = %q; want READY", rec.Status)
	}

	if rec.Route != "transactions" {
		t.Fatalf("Router = %q; want transactions", rec.Route)
	}

	if len(store.saved) != 1 {
		t.Fatalf("Store.Save called %d times; want 1", len(store.saved))
	}
}
