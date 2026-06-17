package main

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// connectMQTT builds the auto-reconnecting MQTT 5.0 connection. Manual
// acknowledgment is enabled so QoS 1/2 messages are acked only after they are
// durably stored in NATS.
func (b *Bridge) connectMQTT(ctx context.Context) (*autopaho.ConnectionManager, error) {
	u, err := url.Parse(b.cfg.MQTT.BrokerURL)
	if err != nil {
		return nil, fmt.Errorf("parse broker url %q: %w", b.cfg.MQTT.BrokerURL, err)
	}

	var password []byte
	if b.cfg.MQTT.PasswordFile != "" {
		p, err := readSecretFile(b.cfg.MQTT.PasswordFile)
		if err != nil {
			return nil, err
		}
		password = []byte(p)
	}

	cliCfg := autopaho.ClientConfig{
		ServerUrls:                    []*url.URL{u},
		KeepAlive:                     uint16(time.Duration(b.cfg.MQTT.KeepAlive).Seconds()),
		CleanStartOnInitialConnection: true, // clean on first connect, persistent thereafter
		SessionExpiryInterval:         uint32(time.Duration(b.cfg.MQTT.SessionExpiry).Seconds()),
		ConnectUsername:               b.cfg.MQTT.Username,
		ConnectPassword:               password,
		OnConnectionUp:                b.onMQTTUp,
		OnConnectionDown: func() bool {
			b.mqttUp.Store(false)
			b.log.Warn("mqtt disconnected")
			return true // keep retrying
		},
		OnConnectError: func(err error) {
			b.log.Warn("mqtt connect error", "err", err)
		},
		ClientConfig: paho.ClientConfig{
			ClientID:                   b.cfg.MQTT.ClientID,
			EnableManualAcknowledgment: true,
			OnPublishReceived: []func(paho.PublishReceived) (bool, error){
				b.handleMQTT,
			},
		},
	}

	return autopaho.NewConnection(ctx, cliCfg)
}

// onMQTTUp (re)subscribes to all topics on every (re)connect.
func (b *Bridge) onMQTTUp(cm *autopaho.ConnectionManager, _ *paho.Connack) {
	if _, err := cm.Subscribe(b.ctx, &paho.Subscribe{
		Subscriptions: []paho.SubscribeOptions{
			{
				Topic: b.cfg.MQTT.TopicFilter,
				QoS:   2,
				// NoLocal stops the broker echoing our own NATS->MQTT publishes.
				NoLocal: true,
				// RetainAsPublished preserves the RETAIN flag so we can mirror
				// retained messages into NATS (brokers clear it otherwise).
				RetainAsPublished: true,
			},
		},
	}); err != nil {
		b.log.Error("mqtt subscribe failed", "filter", b.cfg.MQTT.TopicFilter, "err", err)
		return
	}
	b.mqttUp.Store(true)
	b.log.Info("mqtt connected and subscribed", "filter", b.cfg.MQTT.TopicFilter)
}

// handleMQTT forwards an MQTT message to NATS. For QoS 1/2 it stores a durable
// copy and only then manually acks the broker (at-least-once / exactly-once).
func (b *Bridge) handleMQTT(pr paho.PublishReceived) (bool, error) {
	p := pr.Packet

	if mqttIsBridged(p) {
		return true, nil // our own NATS->MQTT echo
	}
	if strings.HasPrefix(p.Topic, "$") {
		return true, nil // reserved MQTT topics ($SYS, ...)
	}

	subject, err := TopicToSubject(p.Topic)
	if err != nil {
		b.log.Warn("drop unmappable mqtt topic", "topic", p.Topic, "err", err)
		return true, nil // cannot represent in NATS; drop
	}

	ctx, cancel := context.WithTimeout(b.ctx, opTimeout)
	defer cancel()

	// Live copy: publish to the natural subject for core NATS subscribers.
	live := nats.NewMsg(subject)
	live.Data = p.Payload
	natsSetMarker(live)
	if err := b.nc.PublishMsg(live); err != nil {
		b.log.Warn("mqtt->nats live publish failed", "subject", subject, "err", err)
		// For QoS>=1 the durable store below governs acking; for QoS0 it's lost.
	}

	// Durable copy for QoS 1/2: store under the reserved prefix and await the
	// JetStream PubAck before acking MQTT, so nothing is lost on crash.
	if p.QoS >= 1 {
		dm := nats.NewMsg(msgsSubjectPrefix + subject)
		dm.Data = p.Payload
		natsSetMarker(dm)

		var opts []jetstream.PublishOpt
		if p.QoS == 2 {
			// (clientID, packet-id) is unique for the message's in-flight
			// lifetime; within the dedup window this makes a post-crash
			// redelivery idempotent (effective exactly-once into NATS).
			opts = append(opts, jetstream.WithMsgID(
				fmt.Sprintf("%s-%d", b.cfg.MQTT.ClientID, p.PacketID)))
		}
		if _, err := b.js.PublishMsg(ctx, dm, opts...); err != nil {
			b.log.Warn("mqtt->nats durable store failed", "subject", subject, "err", err)
			return false, err // do NOT ack: the broker will redeliver
		}
	}

	if p.Retain && retainedEnabled {
		b.storeRetained(ctx, subject, p.Payload)
	}

	// Manual ack: the message is now safely in NATS.
	if p.QoS >= 1 {
		if err := pr.Client.Ack(p); err != nil {
			b.log.Warn("mqtt ack failed", "topic", p.Topic, "err", err)
		}
	}
	return true, nil
}
