package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Reserved bridge namespace and hardcoded defaults. These mirror the NATS
// server's own MQTT design ($MQTT.* prefix, per-subject streams) and are kept
// out of the config surface for v1 — sensible defaults, exposed later if needed.
const (
	bridgePrefix    = "$MQTT2NATS"
	msgsStreamName  = "MQTT2NATS_MSGS"
	rmsgsStreamName = "MQTT2NATS_RMSGS"

	streamMaxAge   = 168 * time.Hour // 7 days
	streamMaxBytes = int64(1) << 30  // 1 GiB
	dedupWindow    = 2 * time.Minute // QoS 2 Nats-Msg-Id dedup window

	natsToMQTTQoS   = byte(0) // NATS->MQTT firehose QoS (core NATS is at-most-once)
	retainedEnabled = true    // bridge MQTT->NATS retained messages

	opTimeout = 10 * time.Second
)

// Subject prefixes derived from bridgePrefix.
const (
	msgsSubjectPrefix  = bridgePrefix + ".msgs."  // "$MQTT2NATS.msgs."
	rmsgsSubjectPrefix = bridgePrefix + ".rmsgs." // "$MQTT2NATS.rmsgs."
)

// Bridge relays messages between an MQTT 5.0 broker and a NATS server.
type Bridge struct {
	cfg Config
	log *slog.Logger

	nc    *nats.Conn
	js    jetstream.JetStream
	msgs  jetstream.Stream
	rmsgs jetstream.Stream
	sub   *nats.Subscription

	cm     *autopaho.ConnectionManager
	health *http.Server

	ctx context.Context

	natsUp atomic.Bool
	mqttUp atomic.Bool
}

// NewBridge constructs a Bridge from config.
func NewBridge(cfg Config, log *slog.Logger) *Bridge {
	return &Bridge{cfg: cfg, log: log}
}

// Ready reports whether both connections are currently up (for /readyz).
func (b *Bridge) Ready() bool { return b.natsUp.Load() && b.mqttUp.Load() }

// Run connects both sides, installs subscriptions, serves health, and blocks
// until ctx is cancelled, then shuts down gracefully.
func (b *Bridge) Run(ctx context.Context) error {
	b.ctx = ctx

	if err := b.connectNATS(); err != nil {
		return fmt.Errorf("connect nats: %w", err)
	}
	if err := b.ensureStreams(ctx); err != nil {
		return fmt.Errorf("ensure streams: %w", err)
	}

	cm, err := b.connectMQTT(ctx)
	if err != nil {
		return fmt.Errorf("connect mqtt: %w", err)
	}
	b.cm = cm

	// Subscribe to NATS last: handleNATS publishes via b.cm, which must be set first.
	if err := b.subscribeNATS(); err != nil {
		return fmt.Errorf("subscribe nats: %w", err)
	}

	b.health = newHealthServer(b.cfg.HTTPAddr, b.Ready)
	go func() {
		if err := b.health.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			b.log.Error("health server", "err", err)
		}
	}()

	b.log.Info("bridge running",
		"nats_url", b.cfg.NATS.URL,
		"mqtt_broker", b.cfg.MQTT.BrokerURL,
		"topic_filter", b.cfg.MQTT.TopicFilter,
		"http_addr", b.cfg.HTTPAddr,
	)

	<-ctx.Done()
	b.log.Info("shutdown signal received")
	return b.Close()
}

// Close performs a bounded graceful shutdown.
func (b *Bridge) Close() error {
	// Flip readiness off first so Kubernetes stops routing to us.
	b.natsUp.Store(false)
	b.mqttUp.Store(false)

	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()

	if b.health != nil {
		_ = b.health.Shutdown(ctx)
	}
	if b.sub != nil {
		_ = b.sub.Drain()
	}
	if b.nc != nil {
		_ = b.nc.Drain()
	}
	if b.cm != nil {
		_ = b.cm.Disconnect(ctx)
	}
	b.log.Info("shutdown complete")
	return nil
}
