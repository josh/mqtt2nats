FROM golang:1.27-alpine3.23@sha256:0908ac9b9319e09d7c238aabe914e0395c51d63c4e3d0ae8c554fda9158a5769 AS builder

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
