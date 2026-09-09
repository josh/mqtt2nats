FROM golang:1.27-alpine3.23@sha256:d9e2f2f07b10cc922da3e80e035c3058810b328d5aef82d2c63680967c5e2ec9 AS builder

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
