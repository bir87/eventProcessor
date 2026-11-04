package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
)

type template struct {
	producerID, receiverID, eventType string
	payload                           any
}

var templates = []template{
	{"transaction-authorizer", "banking-app", "transaction.authorized", map[string]any{"transaction_id": "tx", "amount": 42.5, "currency": "USD"}},
	{"monitoring-agent", "operations", "monitoring.alert", map[string]any{"severity": "high", "message": "CPU high"}},
	{"user-service", "customer-app", "user.created", map[string]any{"user_id": "u", "email": "demo@example.com"}},
}

func main() {
	broker := flag.String("broker", "localhost:9092", "kafka broker")
	topic := flag.String("topic", "platform.events", "topic")
	count := flag.Int("count", 6, "number of valid events to produce")
	invalid := flag.Bool("invalid", true, "one invalid event")
	flag.Parse()

	w := &kafka.Writer{
		Addr:         kafka.TCP(*broker),
		Topic:        *topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
	}

	defer w.Close()

	for i := 0; i < *count; i++ {
		t := templates[i%len(templates)]
		send(w, map[string]any{
			"producer_id":    t.producerID,
			"receiver_id":    t.receiverID,
			"event_id":       fmt.Sprintf("evt-%d-%d", time.Now().UnixNano(), i),
			"event_type":     t.eventType,
			"schema_version": "v1",
			"tenant_id":      "tenant-demo",
			"occured_at":     time.Now().UTC(),
			"payload":        t.payload,
		})
	}

	if *invalid {
		send(w, map[string]any{
			"producer_id":    "monitoring-agent",
			"receiver_id":    "operations",
			"event_id":       fmt.Sprintf("evt-invalid-%d", time.Now().UnixNano()),
			"event_type":     "monitoring.alert",
			"schema_version": "v1",
			"tenant_id":      "tenant-demo",
			"occured_at":     time.Now().UTC(),
			"payload":        map[string]any{"message": "missing severity field"},
		})
	}
}

func send(w *kafka.Writer, evt map[string]any) {
	b, err := json.Marshal(evt)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	if err := w.WriteMessages(context.Background(), kafka.Message{Key: []byte("tenant-demo"), Value: b}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(string(b))
}
