package main

import (
	"context"
	"eventprocessor/internal/consumer"
	"eventprocessor/internal/processor"
	"eventprocessor/internal/repository"
	"eventprocessor/internal/routing"
	"eventprocessor/internal/validation"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
)

func env(key, def string) string {
	if s := os.Getenv(key); s != "" {
		return s
	}
	return def
}

func main() {
	if err := run(); err != nil {
		slog.Error("process failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	log := slog.Default()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	dbConfig, err := pgxpool.ParseConfig(env("DATABASE_URL", "postgres://events:events@localhost:5432/events?sslmode=disable"))
	if err != nil {
		return fmt.Errorf("invalid database configuration: %w", err)
	}

	pool, err := pgxpool.NewWithConfig(ctx, dbConfig)
	if err != nil {
		return fmt.Errorf("create db pool: %w", err)
	}
	defer pool.Close()

	for {

		if ctx.Err() != nil {
			return nil
		}
		pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := pool.Ping(pingCtx)
		cancel()
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			return nil
		}
		log.Warn("waiting for postgres", "error", err)

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(time.Second):
		}
	}

	if ctx.Err() != nil {
		return nil
	}

	validator, err := validation.New(os.DirFS(env("SCHEMA_DIR", "schemas")))
	if err != nil {
		return fmt.Errorf("schema load failure: %w", err)
	}

	router, err := routing.New(os.DirFS(env("CONFIG_DIR", "config")), "routes.json")
	if err != nil {
		return fmt.Errorf("routing config load failure: %w", err)
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{env("KAFKA_BROKER", "localhost:19092")},
		Topic:          env("KAFKA_TOPIC", "platform.events"),
		GroupID:        env("KAFKA_GROUP", "event-processor"),
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: 0,
		StartOffset:    kafka.FirstOffset})

	c := consumer.Consumer{Reader: reader, Handler: &processor.Processor{Validator: validator, Router: router, Store: &repository.Postgres{Pool: pool}}, Logger: log}
	log.Info("processor started")

	if err := c.Run(ctx); err != nil {
		return fmt.Errorf("processor stopped with error: %w", err)
	}
	return nil
}
