package consumer

import (
	"context"
	"errors"
	"eventprocessor/internal/model"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

type fakeReader struct {
	message   kafka.Message
	fetched   bool
	committed []kafka.Message
	closed    bool
	cancel    context.CancelFunc
}

func (f *fakeReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	if !f.fetched {
		f.fetched = true
		return f.message, nil
	}
	f.cancel()
	return kafka.Message{}, ctx.Err()
}

func (f *fakeReader) CommitMessages(_ context.Context, msgs ...kafka.Message) error {
	f.committed = append(f.committed, msgs...)
	return nil
}

func (f *fakeReader) Close() error {
	f.closed = true
	return nil
}

type handlerFunc func(context.Context, []byte, model.Source) (model.Record, bool, error)

func (fn handlerFunc) Process(ctx context.Context, raw []byte, src model.Source) (model.Record, bool, error) {
	return fn(ctx, raw, src)
}

func TestConsumer_CommitAfterProcessing(t *testing.T) {
	saveErr := errors.New("database unavailable")

	tests := []struct {
		name        string
		processErr  error
		wantCommits int
	}{
		{"success commits", nil, 1},
		{"failure does not commit", saveErr, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			reader := &fakeReader{
				message: kafka.Message{
					Topic:     "platform.events",
					Partition: 0,
					Offset:    42,
					Value:     []byte(`{"event_id":"evt-1"}`),
				},
				cancel: cancel,
			}

			calls := 0
			handler := handlerFunc(func(_ context.Context, _ []byte, src model.Source) (model.Record, bool, error) {
				calls++
				if len(reader.committed) != 0 {
					t.Fatal("message committed before processing complete")
				}

				if tt.processErr != nil {
					cancel()
				}
				return model.Record{Source: src}, false, tt.processErr
			})
			c := Consumer{
				Reader:  reader,
				Handler: handler,
				Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
			}
			err := c.Run(ctx)

			if err != nil {
				t.Fatalf("Run() error = %v; want clean shutdown", err)
			}

			if calls != 1 {
				t.Fatalf("Process called %d times; want 1", calls)
			}

			if len(reader.committed) != tt.wantCommits {
				t.Fatalf("commits = %d; want %d",
					len(reader.committed),
					tt.wantCommits,
				)
			}

			if tt.wantCommits == 1 {
				got := reader.committed[0]
				if got.Topic != reader.message.Topic ||
					got.Partition != reader.message.Partition ||
					got.Offset != reader.message.Offset {
					t.Fatalf("committed wrong message: %+v", got)
				}
			}
			if !reader.closed {
				t.Fatal("reader was not closed")
			}
		})
	}
}

func TestConsumer_RetryPolicy(t *testing.T) {
	cause := errors.New("storage failure")

	tests := []struct {
		name        string
		retryable   bool
		wantCalls   int
		wantCommits int
	}{
		{
			name:        "retryable failure followed by success",
			retryable:   true,
			wantCalls:   2,
			wantCommits: 1,
		},
		{
			name:        "non-retryable failure stops without cancellation",
			retryable:   false,
			wantCalls:   1,
			wantCommits: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(
				context.Background(), 5*time.Second,
			)
			defer cancel()

			reader := &fakeReader{
				message: kafka.Message{
					Topic:     "platform.events",
					Partition: 0,
					Offset:    42,
					Value:     []byte(`{"event_id":"evt-1"}`),
				},
				cancel: cancel,
			}
			calls := 0
			handler := handlerFunc(func(_ context.Context, _ []byte, src model.Source,
			) (model.Record, bool, error) {
				calls++
				if len(reader.committed) != 0 {
					t.Fatal("message committed before processing succeeded")
				}

				record := model.Record{Source: src}
				if calls == 1 {
					return record, false, &model.StorageError{
						Err:       cause,
						Retryable: tt.retryable,
					}
				}
				return record, false, nil
			})
			consumer := Consumer{
				Reader: reader, Handler: handler,
				Logger: slog.New(
					slog.NewTextHandler(io.Discard, nil),
				),
			}
			err := consumer.Run(ctx)
			if tt.retryable {
				if err != nil {
					t.Fatalf("Run() error = %v; want nil", err)
				}
			} else {
				if !errors.Is(err, cause) {
					t.Fatalf("Run() error = %v; want %v", err, cause)
				}
				if ctx.Err() != nil {
					t.Fatalf("consumer stopped through cancellation: %v", ctx.Err())
				}
			}
			if calls != tt.wantCalls {
				t.Fatalf("Process calls = %d; want %d", calls, tt.wantCalls)
			}
			if len(reader.committed) != tt.wantCommits {
				t.Fatalf("commits = %d; want %d",
					len(reader.committed), tt.wantCommits,
				)
			}
			if !reader.closed {
				t.Fatal("reader was not closed")
			}
		})
	}
}
