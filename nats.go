package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// connectNATS dials NATS with resilient reconnection and the configured auth,
// then initialises the JetStream context.
func (b *Bridge) connectNATS() error {
	opts := []nats.Option{
		nats.Name("mqtt2nats"),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2 * time.Second),
		nats.RetryOnFailedConnect(true),
		// Suppress delivery of our own publishes back to our ">" subscription;
		// the loop marker is the cross-replica guarantee, this just avoids churn.
		nats.NoEcho(),
		nats.ReconnectHandler(func(*nats.Conn) {
			b.natsUp.Store(true)
			b.log.Info("nats reconnected")
		}),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			b.natsUp.Store(false)
			b.log.Warn("nats disconnected", "err", err)
		}),
		nats.ClosedHandler(func(*nats.Conn) {
			b.natsUp.Store(false)
		}),
	}

	authOpt, err := natsAuthOption(b.cfg.NATS)
	if err != nil {
		return err
	}
	if authOpt != nil {
		opts = append(opts, authOpt)
	}

	nc, err := nats.Connect(b.cfg.NATS.URL, opts...)
	if err != nil {
		return err
	}
	b.nc = nc
	b.natsUp.Store(nc.IsConnected())

	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}
	b.js = js
	return nil
}

// natsAuthOption selects the auth method from whichever credential file is set,
// in precedence order: creds file, token, user+password.
func natsAuthOption(c NATSConfig) (nats.Option, error) {
	switch {
	case c.CredsFile != "":
		return nats.UserCredentials(c.CredsFile), nil
	case c.TokenFile != "":
		tok, err := readSecretFile(c.TokenFile)
		if err != nil {
			return nil, err
		}
		return nats.Token(tok), nil
	case c.UserFile != "" || c.PasswordFile != "":
		var user, pass string
		var err error
		if c.UserFile != "" {
			if user, err = readSecretFile(c.UserFile); err != nil {
				return nil, err
			}
		}
		if c.PasswordFile != "" {
			if pass, err = readSecretFile(c.PasswordFile); err != nil {
				return nil, err
			}
		}
		return nats.UserInfo(user, pass), nil
	default:
		return nil, nil
	}
}

// ensureStreams creates (or updates) the bridge-owned streams under the reserved
// $MQTT2NATS prefix. They never capture system or user subjects.
func (b *Bridge) ensureStreams(ctx context.Context) error {
	msgs, err := b.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:        msgsStreamName,
		Description: "mqtt2nats: durable copy of bridged MQTT messages (QoS 1/2)",
		Subjects:    []string{msgsSubjectPrefix + ">"},
		Storage:     jetstream.FileStorage,
		Retention:   jetstream.LimitsPolicy,
		MaxAge:      streamMaxAge,
		MaxBytes:    streamMaxBytes,
		Duplicates:  dedupWindow,
	})
	if err != nil {
		return fmt.Errorf("msgs stream: %w", err)
	}
	b.msgs = msgs

	if retainedEnabled {
		rmsgs, err := b.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name:              rmsgsStreamName,
			Description:       "mqtt2nats: retained last-value per topic (MQTT->NATS)",
			Subjects:          []string{rmsgsSubjectPrefix + ">"},
			Storage:           jetstream.FileStorage,
			Retention:         jetstream.LimitsPolicy,
			MaxMsgsPerSubject: 1,
		})
		if err != nil {
			return fmt.Errorf("rmsgs stream: %w", err)
		}
		b.rmsgs = rmsgs
	}
	return nil
}

// subscribeNATS installs the core ">" firehose subscription (NATS -> MQTT).
func (b *Bridge) subscribeNATS() error {
	sub, err := b.nc.Subscribe(">", b.handleNATS)
	if err != nil {
		return err
	}
	b.sub = sub
	return nil
}

// handleNATS forwards a NATS message to MQTT, skipping system/protocol subjects
// and any message already bridged (loop prevention).
func (b *Bridge) handleNATS(m *nats.Msg) {
	if strings.HasPrefix(m.Subject, "$") || strings.HasPrefix(m.Subject, "_INBOX.") {
		return // NATS system / protocol traffic, not application data
	}
	if natsIsBridged(m) {
		return // MQTT-origin echo
	}

	topic := SubjectToTopic(m.Subject)
	props := &paho.PublishProperties{}
	mqttSetMarker(props)

	pub := &paho.Publish{
		QoS:        natsToMQTTQoS,
		Topic:      topic,
		Payload:    m.Data,
		Properties: props,
	}

	ctx, cancel := context.WithTimeout(b.ctx, opTimeout)
	defer cancel()
	if _, err := b.cm.Publish(ctx, pub); err != nil {
		b.log.Warn("nats->mqtt publish failed", "topic", topic, "err", err)
	}
}
