FROM golang:1.27rc2-alpine3.23@sha256:ae3270df82e02c950e7cc87b8e3c5d8f25a9c9006de73fa029f27b4cdcecc68f AS builder

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
