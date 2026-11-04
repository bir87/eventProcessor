package repository

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"eventprocessor/internal/model"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Postgres struct{ Pool *pgxpool.Pool }

var ErrConflictingEvent = errors.New("conflicting event: an event with the same ID but different content already exists in the database")

func isTransient(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return true
	}

	switch pgErr.Code[:2] {
	case "08", "53", "57", "40":
		return true
	default:
		return false
	}
}

func (p *Postgres) Save(ctx context.Context, r model.Record) (quarantined bool, err error) {
	errs, _ := json.Marshal(r.Errors)
	var payload any
	if json.Valid(r.Envelope.Payload) {
		payload = string(r.Envelope.Payload)
	}

	tx, err := p.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return false, err
	}

	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, insertEventSQL, r.Envelope.EventID, r.Envelope.ProducerID, r.Envelope.ReceiverID,
		r.Envelope.EventType, r.Envelope.SchemaVersion, r.Envelope.TenantID, r.Envelope.OccuredAt, payload,
		r.Raw, r.Status, errs, r.Route, r.Source.Topic, r.Source.Partition, r.Source.Offset)
	if err != nil {
		if isTransient(err) {
			return false, err
		}
		tx.Rollback(ctx)
		if _, qErr := p.Pool.Exec(ctx, insertQuarantineSQL, r.Source.Topic, r.Source.Partition, r.Source.Offset, nil, nil, r.Raw, "permanent persistence err: "+err.Error()); qErr != nil {
			return false, fmt.Errorf("quarantine permanent failure: %w", qErr)
		}
		return true, nil
	}

	if tag.RowsAffected() == 0 {
		var oldRaw []byte
		err = tx.QueryRow(ctx, selectRAWByOffsetSQL, r.Source.Topic, r.Source.Partition, r.Source.Offset).Scan(&oldRaw)
		if errors.Is(err, pgx.ErrNoRows) && r.Envelope.EventID != "" && r.Envelope.TenantID != "" {
			err = tx.QueryRow(ctx, selectRawByIdentitySQL, r.Envelope.TenantID, r.Envelope.EventID).Scan(&oldRaw)
		}

		if err != nil {
			return false, fmt.Errorf("deduplication lookup: %w", err)
		}

		if !bytes.Equal(oldRaw, r.Raw) {
			_, err = tx.Exec(ctx, insertQuarantineSQL,
				r.Source.Topic,
				r.Source.Partition,
				r.Source.Offset,
				r.Envelope.TenantID,
				r.Envelope.EventID,
				r.Raw,
				ErrConflictingEvent.Error())
			if err != nil {
				return false, fmt.Errorf("quarantine conflicting events: %w", err)
			}
			quarantined = true
		}
	}

	return quarantined, tx.Commit(ctx)
}
