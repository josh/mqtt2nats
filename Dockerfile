FROM golang:1.26.4-alpine3.23@sha256:f23e8b227fb4493eabe03bede4d5a32d04092da71962f1fb79b5f7d1e6c2a17f AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -mod=readonly -ldflags="-s -w" -o mqtt2nats .

FROM scratch

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /src/mqtt2nats /usr/local/bin/

LABEL org.opencontainers.image.source="https://github.com/josh/mqtt2nats"
LABEL org.opencontainers.image.description="Bidirectional NATS <-> MQTT 5.0 bridge"
LABEL org.opencontainers.image.licenses="MIT"

ENTRYPOINT ["mqtt2nats"]
