# Event Processor

## Prerequisites

- Git
- Docker with Docker compose
- Go matching verion in `go.mod`

Docker must be running, lcoal env uses `5432` for postgres and `19902` for kafka.

## Clone the repo

```bash
git clone https://github.com/bir87/eventProcessor.git
cd eventProcessor
```

## Start the environment
```bash
make up
docker compose ps
```
This builds the app and starts kafka, postgresql, topic init and processor.

## Produce Sample Events
```bash
make produce
```

## Inspect Processing logs
```bash
docker compose logs --tail=100 processor
docker compose logs -f processor
```

## Verify Stored Events
```bash
docker compose exec postgres psql -U events -d events -P pager=off -c \
"SELECT status, COUNT(*) FROM events GROUP BY status ORDER BY status;"
```
Inspect validation failures:

```bash
docker compose exec postgres psql -U events -d events -P pager=off -c \
"SELECT event_id, validation_errors FROM events WHERE status = 'INVALID';"
```

Inspect routing decisions:

```bash
docker compose exec postgres psql -U events -d events -P pager=off -c \
"SELECT event_id, event_type, status, routing_target FROM events ORDER BY id;"
```

Inspect quarantined events:

```bash
docker compose exec postgres psql -U events -d events -P pager=off -c \
"SELECT source_topic, source_partition, source_offset, reason FROM event_quarantine ORDER BY id;"
```
## Run Automated Tests

```bash
make test
```

To run only the database tests:

```bash
make test-db
```

## Stop or Reset

Stop and remove containers while retaining named volumes:

```bash
make down
```

Delete containers and persisted demo data:

```bash
make clean
```

Start a fresh environment:

```bash
make clean
make up
make produce
```

**`make clean` deletes Kafka and PostgreSQL data, including the test database.**