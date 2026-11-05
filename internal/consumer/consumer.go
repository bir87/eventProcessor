package consumer

import (
	"context"
	"eventprocessor/internal/model"
	"fmt"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
)

type Handler interface {
	Process(context.Context, []byte, model.Source) (model.Record, bool, error)
}

type MessageReader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}
type Consumer struct {
	Reader  MessageReader
	Handler Handler
	Logger  *slog.Logger
}

func (c *Consumer) Run(ctx context.Context) error {
	defer c.Reader.Close()
	for {
		m, err := c.Reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}

		src := model.Source{
			Topic:     m.Topic,
			Partition: m.Partition,
			Offset:    m.Offset,
		}
		c.Logger.Info("event received", "topic", m.Topic, "partition", m.Partition, "offset", m.Offset)

		for attempt := 0; ; attempt++ {
			if ctx.Err() != nil {
				return nil
			}

			rec, quarantined, procErr := c.Handler.Process(ctx, m.Value, src)
			err = procErr
			if err == nil {
				if quarantined {
					c.Logger.Warn("event quarantined", "topic", m.Topic, "partition", m.Partition, "offset", m.Offset, "tenant_id",
						rec.Envelope.TenantID, "event_id", rec.Envelope.EventID)
				} else {
					c.Logger.Info("event processed", "topic", m.Topic, "partition", m.Partition, "offset", m.Offset, "tenant_id",
						rec.Envelope.TenantID, "event_id", rec.Envelope.EventID, "status", rec.Status, "route", rec.Route, "validation_errors", rec.Errors)
				}
				break
			}

			if ctx.Err() != nil {
				return nil
			}

			if !model.IsRetryable(err) {
				return fmt.Errorf(
					"non-retryable processing failure at %s/%d/%d: %w",
					m.Topic, m.Partition, m.Offset, err,
				)
			}

			c.Logger.Error("event persistence failed; retrying",
				"topic", m.Topic, "partition", m.Partition,
				"offset", m.Offset, "tenant_id", rec.Envelope.TenantID,
				"event_id", rec.Envelope.EventID,
				"attempt", attempt+1, "error", err)
			delay := time.Duration(1<<min(attempt, 5)) * time.Second

			select {
			case <-ctx.Done():
				return nil
			case <-time.After(delay):
			}
		}

		commitCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err = c.Reader.CommitMessages(commitCtx, m)
		cancel()
		if err != nil {
			return err
		}
		c.Logger.Info("offset committed", "topic", m.Topic, "partition", m.Partition, "offset", m.Offset)
		if ctx.Err() != nil {
			return nil
		}
	}
}
