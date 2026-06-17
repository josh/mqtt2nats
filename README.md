# mqtt2nats

A small Go service that bridges an **MQTT 5.0** broker and a **NATS** server,
relaying every message in both directions.

NATS's built-in MQTT support speaks only MQTT 3.1.1. mqtt2nats connects as a
_client_ to an external MQTT 5.0 broker (EMQX, HiveMQ, Mosquitto, …) and as a
_client_ to a NATS server, giving a NATS cluster access to MQTT 5.0 data.

- **Bidirectional, no config filtering** — every MQTT message is published to
  NATS and every NATS message is published to MQTT.
- **Loop-safe** — every bridged message is tagged (`X-Mqtt2Nats-Bridged`); each
  side drops anything already tagged, so messages never cycle.
- **NATS-faithful mapping** — MQTT topics map to NATS subjects exactly as a
  native NATS MQTT deployment would (`home/room/temp` ⇄ `home.room.temp`, a
  literal `.` ⇄ `//`, etc.).
- **Durable MQTT→NATS (QoS 0/1/2)** — each MQTT message is published to its
  natural NATS subject for live consumers, and (for QoS ≥ 1) stored in a
  bridge-owned JetStream stream and only acked once safely persisted. QoS 2 is
  deduplicated for effective exactly-once.
- **Retained** — MQTT retained messages are mirrored to a last-value-per-subject
  JetStream stream (an empty payload clears it).

## How messages flow

mqtt2nats mirrors the NATS server's own MQTT design: a reserved `$MQTT2NATS.`
subject namespace, dual-publish, and per-subject JetStream streams that never
capture system or user subjects.

**MQTT → NATS** (primary). For each message from the broker:

- published to the **natural subject** (e.g. `home.room.temp`) for live/core
  NATS subscribers;
- for QoS 1/2, also stored in `MQTT2NATS_MSGS` (`$MQTT2NATS.msgs.<subject>`) and
  acked to the broker only after the JetStream `PubAck` — nothing is lost on a
  crash. QoS 2 carries a `Nats-Msg-Id` so a post-crash redelivery is idempotent;
- if `RETAIN` is set, the last value is upserted into `MQTT2NATS_RMSGS`
  (`$MQTT2NATS.rmsgs.<subject>`); an empty payload deletes it.

**NATS → MQTT** (firehose). A core `>` subscription forwards every application
message to MQTT at QoS 0, skipping `$`-prefixed and `_INBOX.` subjects (NATS
protocol/system traffic) and anything already bridged.

## Requirements

- An MQTT **5.0** broker.
- A NATS server with **JetStream** enabled, in an **account dedicated to this
  service** (the bridge owns the `$MQTT2NATS.*` streams and a core `>`
  subscription).

## Install

```sh
go install github.com/josh/mqtt2nats@latest
```

Or clone and `go build .`.

## Configuration

A single JSON file. Path resolution, in order:

1. `-config <path>` flag
2. `MQTT2NATS_CONFIG` env var
3. `./mqtt2nats.json`

`mqtt2nats -print-config` prints the merged (defaults + overrides) config — a
handy template. `-verbose` enables debug logging.

Secrets are referenced by **file path** (mount them from Kubernetes Secrets),
never inline. Example `mqtt2nats.json`:

```json
{
  "mqtt": {
    "broker_url": "tls://broker:8883",
    "client_id": "mqtt2nats",
    "username": "bridge",
    "password_file": "/etc/mqtt2nats/secrets/mqtt.password",
    "topic_filter": "#",
    "session_expiry": "24h",
    "keepalive": "30s"
  },
  "nats": {
    "url": "nats://nats:4222",
    "creds_file": "/etc/mqtt2nats/secrets/nats.creds"
  },
  "http_addr": ":8080"
}
```

Only `mqtt.broker_url` and `nats.url` are required. NATS auth is one of
`creds_file`, `token_file`, or `user_file`+`password_file`.

`http_addr` serves Kubernetes probes: `/healthz` (liveness — up while the
process is alive) and `/readyz` (readiness — up only when both connections are).

## Deployment

A Helm chart is included:

```sh
helm install mqtt2nats ./charts/mqtt2nats \
  --set config.mqtt.broker_url=tls://broker:8883 \
  --set config.nats.url=nats://nats:4222
```

The chart renders `config.json` into a ConfigMap, mounts referenced Secrets as
files, and runs a single replica with a read-only root filesystem and a
non-root, all-capabilities-dropped security context. See
[`charts/mqtt2nats/values.yaml`](charts/mqtt2nats/values.yaml).

## Testing

Tests run fully in-process — an embedded NATS server (JetStream) and an embedded
MQTT 5.0 broker are started in the test binary. No Docker, no external services:

```sh
go test -race ./...
```

## Scope (v1)

Single instance. Multi-replica HA (MQTT shared subscriptions + shared NATS
durable consumers), reliable QoS 1/2 on the NATS→MQTT direction, and NATS→MQTT
retained are deliberately out of scope for now.
