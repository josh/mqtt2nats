FROM golang:1.27-alpine3.23@sha256:4441ef16de1cbb69a44ab7c3cadc2c4b85d6e63494a4c0df252c5aae6204b865 AS builder

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
