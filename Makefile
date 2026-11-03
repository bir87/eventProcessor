.PHONY: up down test produce clean
up:
	docker compose up --build -d

produce:
	docker compose --profile tools run --build --rm producer
	
produce-local:
	go run ./cmd/producer --broker localhost:19902 - count 6

test:
	go test ./...

down:
	docker compose down

clean:
	docker compose down -v