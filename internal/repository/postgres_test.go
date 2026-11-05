package repository

import (
	"bytes"
	"context"
	"encoding/json"
	"eventprocessor/internal/model"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func setupTestDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pgname := os.Getenv("TEST_DATABASE_URL")
	if pgname == "" {
		t.Skip("set TEST_DATABASE_URL postgres service")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, pgname)
	if err != nil {
		t.Fatalf("pgxpool.New() error = %v", err)
	}
	t.Cleanup(pool.Close)
	if pool.Config().ConnConfig.Database != "events_test" {
		t.Fatal("repo tests must use events_test, setup truncates tables")
	}

	migration, err := os.ReadFile("../../migrations/001_create_events.sql")
	if err != nil {
		t.Fatalf("read migration file: %v", err)
	}

	for _, stmt := range strings.Split(string(migration), ";") {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		if _, err := pool.Exec(ctx, stmt); err != nil {
			t.Fatalf("apply migration statement %q: %v", stmt, err)
		}
	}
	if _, err := pool.Exec(ctx, "TRUNCATE events, event_quarantine RESTART IDENTITY"); err != nil {
		t.Fatalf("truncate tables: %v", err)
	}

	return pool
}

func countRows(t *testing.T, pool *pgxpool.Pool, table string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM "+table).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

func baseRecord(src model.Source, raw []byte) model.Record {
	return model.Record{
		Envelope: model.Envelope{
			ProducerID:    "producer-1",
			ReceiverID:    "receiver-1",
			EventID:       "evt-1",
			EventType:     "transaction.authorized",
			SchemaVersion: "v1",
			TenantID:      "tenant-demo",
			OccuredAt:     time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		Raw:    raw,
		Source: src,
		Status: string(model.StatusReady),
		Errors: []string{},
		Route:  "transactions",
	}
}

func invalidRecord(src model.Source, raw []byte) model.Record {
	rec := baseRecord(src, raw)
	rec.Status = string(model.StatusInvalid)
	rec.Errors = []string{model.ErrSchemaValidation + "amount is required"}
	rec.Route = ""
	return rec
}

func TestSave(t *testing.T) {
	cases := []struct {
		name            string
		records         []model.Record
		wantQuarantined []bool
		wantEvents      int
		wantQuarantine  int
		check           func(t *testing.T, pool *pgxpool.Pool)
	}{
		{
			name:            "persists new event",
			records:         []model.Record{baseRecord(model.Source{Topic: "t", Partition: 0, Offset: 1}, []byte(`{"a":1}`))},
			wantQuarantined: []bool{false},
			wantEvents:      1,
		},
		{
			name:            "invalid event is persisted, not dropped",
			records:         []model.Record{invalidRecord(model.Source{Topic: "t", Partition: 0, Offset: 1}, []byte(`{"not":"schema-schema"}`))},
			wantQuarantined: []bool{false},
			wantEvents:      1,
			check: func(t *testing.T, pool *pgxpool.Pool) {
				var status string
				var errsRaw []byte
				if err := pool.QueryRow(context.Background(), "SELECT status, validation_errors FROM events LIMIT 1").
					Scan(&status, &errsRaw); err != nil {
					t.Fatalf("read back invalid event: %v", err)
				}
				if status != string(model.StatusInvalid) {
					t.Fatalf("status = %q; want INVALID", status)
				}
				var errs []string
				if err := json.Unmarshal(errsRaw, &errs); err != nil {
					t.Fatalf("unmarshal validation_errors: %v", err)
				}
				if len(errs) == 0 {
					t.Fatal("validation_errors = []; want the recorded error to complete the round trip")
				}
			},
		},
		{
			name: "same offset replay is idempotent",
			records: func() []model.Record {
				rec := baseRecord(model.Source{Topic: "t", Partition: 0, Offset: 1}, []byte(`{"a":1}`))
				return []model.Record{rec, rec}
			}(),
			wantQuarantined: []bool{false, false},
			wantEvents:      1,
		},
		{
			name: "same identity diff offset same content is dedup",
			records: func() []model.Record {
				raw := []byte(`{"a":1}`)
				return []model.Record{
					baseRecord(model.Source{Topic: "t", Partition: 0, Offset: 1}, raw),
					baseRecord(model.Source{Topic: "t", Partition: 0, Offset: 2}, raw),
				}
			}(),
			wantQuarantined: []bool{false, false},
			wantEvents:      1,
			wantQuarantine:  0,
		},
		{
			name: "same identity diff offset conflict with content is marked as quarantine",
			records: []model.Record{
				baseRecord(model.Source{Topic: "t", Partition: 0, Offset: 1}, []byte(`{"a":1}`)),
				baseRecord(model.Source{Topic: "t", Partition: 0, Offset: 2}, []byte(`{"a":2}`)),
			},
			wantQuarantined: []bool{false, true},
			wantEvents:      1,
			wantQuarantine:  1,
			check: func(t *testing.T, pool *pgxpool.Pool) {
				var reason string
				if err := pool.QueryRow(context.Background(), "SELECT reason FROM event_quarantine LIMIT 1").
					Scan(&reason); err != nil {
					t.Fatalf("read quarantine reason: %v", err)
				}
				if !strings.Contains(reason, "conflicting event") {
					t.Errorf("quarantine reason = %q; shoudl have a conflicting event", reason)
				}
			},
		},
		{
			name: "same event ID belongs to idependet to different tenants",
			records: func() []model.Record {
				first := baseRecord(
					model.Source{Topic: "t", Partition: 0, Offset: 1},
					nil,
				)
				first.Envelope.TenantID = "tenant-a"
				first.Envelope.Payload = json.RawMessage(
					`{"transaction_id":"tx-1", "amount":10,"currency":"USD"}`,
				)
				second := first
				second.Envelope.TenantID = "tenant-b"
				second.Source.Offset = 2
				records := []model.Record{first, second}
				for i := range records {
					raw, err := json.Marshal(records[i].Envelope)
					if err != nil {
						t.Fatalf("marshal tenant test events: %v", err)
					}
					records[i].Raw = raw
				}
				return records
			}(),
			wantQuarantined: []bool{false, false},
			wantEvents:      2,
			wantQuarantine:  0,
			check: func(t *testing.T, pool *pgxpool.Pool) {
				for _, tenant := range []string{"tenant-a", "tenant-b"} {
					var count int
					err := pool.QueryRow(
						context.Background(),
						"SELECT count(*) FROM events WHERE tenant_id=$1 AND event_id=$2", tenant, "evt-1",
					).Scan(&count)
					if err != nil {
						t.Fatalf("query tenant %s: %v", tenant, err)
					}
					if count != 1 {
						t.Fatalf("tenant %s has %d events; want 1", tenant, count)
					}
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pool := setupTestDB(t)
			repo := &Postgres{Pool: pool}
			ctx := context.Background()

			for i, rec := range tc.records {
				quarantined, err := repo.Save(ctx, rec)
				if err != nil {
					t.Fatalf("Save() %d error = %v", i, err)
				}
				if quarantined != tc.wantQuarantined[i] {
					t.Fatalf("Save() %d quarantined = %v; want %v", i, quarantined, tc.wantQuarantined[i])
				}
			}
			if got := countRows(t, pool, "events"); got != tc.wantEvents {
				t.Fatalf("events rows = %d; want %d", got, tc.wantEvents)
			}

			if got := countRows(t, pool, "event_quarantine"); got != tc.wantQuarantine {
				t.Fatalf("event_quarantine rows = %d; want %d", got, tc.wantQuarantine)
			}
			if tc.check != nil {
				tc.check(t, pool)
			}
		})
	}
}

func TestSave_PoisonEventQuarantinedThenValidEventSaved(t *testing.T) {
	pool := setupTestDB(t)
	repo := &Postgres{Pool: pool}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	poison := baseRecord(model.Source{Topic: "t", Partition: 0, Offset: 1}, nil)
	poison.Envelope.EventID = "evt-poison"
	poison.Envelope.Payload = json.RawMessage(`{"transaction_id":"tx-\u0000", "amount":10, "currency":"USD"}`)
	raw, err := json.Marshal(poison.Envelope)
	if err != nil {
		t.Fatalf("marshal poison event: %v", err)
	}
	poison.Raw = raw

	quarantined, err := repo.Save(ctx, poison)
	if err != nil {
		t.Fatalf("save poison event: %v", err)
	}
	if !quarantined {
		t.Fatal("poison event was not quarantined")
	}

	var storedRaw []byte
	var reason string
	err = pool.QueryRow(ctx, `SELECT raw_payload, reason FROM event_quarantine WHERE source_topic=$1 AND source_partition=$2 AND source_offset=$3`,
		poison.Source.Topic, poison.Source.Partition, poison.Source.Offset).Scan(&storedRaw, &reason)
	if err != nil {
		t.Fatalf("read quarantined event: %v", err)
	}
	if !bytes.Equal(storedRaw, poison.Raw) {
		t.Error("quarantine did not save the original raw bytes")
	}

	valid := baseRecord(model.Source{Topic: "t", Partition: 0, Offset: 2}, nil)
	valid.Envelope.EventID = "evt-valid"
	valid.Envelope.Payload = json.RawMessage(`{"transaction_id":"tx-2", "amount":10, "currency":"USD"}`)

	raw, err = json.Marshal(valid.Envelope)
	if err != nil {
		t.Fatalf("marshal valid event: %v", err)
	}
	valid.Raw = raw

	quarantined, err = repo.Save(ctx, valid)
	if err != nil {
		t.Fatalf("save valid event after posion event: %v", err)
	}

	if quarantined {
		t.Fatal("valid event was unexpectedly quarantined")
	}

	if got := countRows(t, pool, "events"); got != 1 {
		t.Errorf("events rwos = %d; want 1", got)
	}

	if got := countRows(t, pool, "event_quarantine"); got != 1 {
		t.Errorf("quarantine rwos = %d; want 1", got)
	}

	var eventID, status string
	err = pool.QueryRow(ctx, "SELECT event_id, status FROM events").Scan(&eventID, &status)
	if err != nil {
		t.Fatalf("read saved valid event: %v", err)
	}
	if eventID != valid.Envelope.EventID || status != string(model.StatusReady) {
		t.Errorf("saved event = (%q, %q); want(%q, READY)", eventID, status, valid.Envelope.EventID)
	}

}
