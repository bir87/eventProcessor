FROM golang:1.27.3-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -o /out/processor ./cmd/processor && CGO_ENABLED=0 go build -o /out/producer ./cmd/producer
FROM alpine:3.21
RUN adduser -D app
COPY --from=build /out/processor /usr/local/bin/processor
COPY --from=build /out/producer /usr/local/bin/producer
COPY schemas ./schemas
COPY config ./config
USER app
CMD ["processor"]