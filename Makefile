.PHONY: up down test produce clean
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

TEST_DATABASE_URL ?= postgres://events:events@localhost:5432/events?sslmode=disable
test:
	docker compose up -d --wait postgres
	TEST_DATABASE_URL="$(TEST_DATABASE_URL)" go test ./...

unit-test:
	go test ./...

vet: 
	go vet ./...

lint:

