package repository

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"eventprocessor/internal/model"
	"fmt"
	"io"
	"net"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Postgres struct{ Pool *pgxpool.Pool }

var ErrConflictingEvent = errors.New("conflicting event: an event with the same ID but different content already exists in the database")

// connection error, serialization, deadlock, too many connections, db not ready etc.
func isTransient(err error) bool {
	if errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		if strings.HasPrefix(pgErr.Code, "08") {
			return true
		}
		switch pgErr.Code {
		case "40001", "40P01", "53300", "57P01", "57P02", "57P03":
			return true
		default:
			return false
		}
	}
	var networkErr net.Error
	return errors.As(err, &networkErr) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}

// missing table, sql syntax errors, permission errors.
func isEventDataError(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}

	switch pgErr.Code {
	case "22021", "22P05":
		return true
	default:
		return false
	}
}

func (p *Postgres) Save(ctx context.Context, r model.Record) (quarantined bool, err error) {
	defer func() {
		if err != nil {
			err = &model.StorageError{
				Err:       err,
				Retryable: isTransient(err),
			}
		}
	}()
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
		if !isEventDataError(err) {
			return false, fmt.Errorf("insert event: %w", err)
		}
		if rollbackErr := tx.Rollback(ctx); rollbackErr != nil {
			return false, fmt.Errorf("rollback before quarantine: %w",
				rollbackErr,
			)
		}

		_, quarantineErr := p.Pool.Exec(
			ctx,
			insertQuarantineSQL, r.Source.Topic, r.Source.Partition,
			r.Source.Offset, nil, nil, r.Raw, "event data cannot be saved: "+err.Error(),
		)

		if quarantineErr != nil {
			return false, fmt.Errorf(
				"quarantine event data: %w", quarantineErr,
			)
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
