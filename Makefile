.PHONY: up down test test-db produce clean

test: test-db
	go test ./internal/consumer ./internal/processor ./internal/routing ./internal/validation -count=1 -v

TEST_DATABASE_URL ?= postgres://events:events@localhost:5432/events_test?sslmode=disable

up:
	docker compose up --build -d

produce:
	docker compose --profile tools run --build --rm producer
	
produce-local:
	go run ./cmd/producer --broker localhost:19092 --count 6

down:
	docker compose down

clean:
	docker compose down -v

test-db:
	docker compose up -d --wait postgres
	printf '%s\n' \
		"SELECT 'CREATE DATABASE events_test' WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'events_test')" \
		'\gexec' | docker compose exec -T postgres psql -U events -d postgres -v ON_ERROR_STOP=1
	TEST_DATABASE_URL="$(TEST_DATABASE_URL)" go test ./internal/repository -count=1 -v

unit-test:
	go test ./...

vet: 
	go vet ./...

fmt:
	gofmt -w .