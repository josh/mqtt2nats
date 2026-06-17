package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	mochi "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// testEnv is a fully in-process stack: an embedded NATS server (JetStream), an
// embedded MQTT 5.0 broker (mochi), and a running bridge wired to both. No
// external processes or Docker are used.
type testEnv struct {
	natsURL string
	mqttURL string
	nc      *nats.Conn
	js      jetstream.JetStream
	bridge  *Bridge
}

func newTestEnv(t *testing.T) *testEnv {
	t.Helper()
	natsURL := startNATS(t)
	mqttURL := startMQTT(t)

	cfg := Config{
		MQTT: MQTTConfig{
			BrokerURL:     mqttURL,
			ClientID:      "mqtt2nats-test",
			TopicFilter:   "#",
			SessionExpiry: Duration(time.Hour),
			KeepAlive:     Duration(10 * time.Second),
		},
		NATS:     NATSConfig{URL: natsURL},
		HTTPAddr: "127.0.0.1:0",
	}

	b := NewBridge(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- b.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-errCh:
		case <-time.After(15 * time.Second):
			t.Error("bridge did not shut down in time")
		}
	})

	eventually(t, 15*time.Second, b.Ready, "bridge never became ready")

	nc, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatalf("test nats connect: %v", err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("test jetstream: %v", err)
	}

	return &testEnv{natsURL: natsURL, mqttURL: mqttURL, nc: nc, js: js, bridge: b}
}

func startNATS(t *testing.T) string {
	t.Helper()
	ns, err := natsserver.NewServer(&natsserver.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		JetStream: true,
		StoreDir:  t.TempDir(),
		NoLog:     true,
		NoSigs:    true,
	})
	if err != nil {
		t.Fatalf("new nats server: %v", err)
	}
	go ns.Start()
	if !ns.ReadyForConnections(10 * time.Second) {
		t.Fatal("nats not ready")
	}
	t.Cleanup(func() {
		ns.Shutdown()
		ns.WaitForShutdown()
	})
	return ns.ClientURL()
}

func startMQTT(t *testing.T) string {
	t.Helper()
	broker := mochi.New(&mochi.Options{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err := broker.AddHook(new(auth.AllowHook), nil); err != nil {
		t.Fatalf("mqtt auth hook: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("mqtt listen: %v", err)
	}
	addr := ln.Addr().String()
	if err := broker.AddListener(listeners.NewNet("t", ln)); err != nil {
		t.Fatalf("mqtt add listener: %v", err)
	}
	go func() { _ = broker.Serve() }()
	t.Cleanup(func() { _ = broker.Close() })
	return "mqtt://" + addr
}

// mqttClient connects a paho v5 client to the embedded broker. onPub (if set)
// is called for every received PUBLISH.
func (e *testEnv) mqttClient(t *testing.T, clientID string, onPub func(*paho.Publish)) *autopaho.ConnectionManager {
	t.Helper()
	u, _ := url.Parse(e.mqttURL)
	cfg := autopaho.ClientConfig{
		ServerUrls:                    []*url.URL{u},
		KeepAlive:                     10,
		CleanStartOnInitialConnection: true,
		ClientConfig: paho.ClientConfig{
			ClientID: clientID,
		},
	}
	if onPub != nil {
		cfg.OnPublishReceived = []func(paho.PublishReceived) (bool, error){
			func(pr paho.PublishReceived) (bool, error) {
				onPub(pr.Packet)
				return true, nil
			},
		}
	}
	ctx := context.Background()
	cm, err := autopaho.NewConnection(ctx, cfg)
	if err != nil {
		t.Fatalf("mqtt client connect: %v", err)
	}
	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := cm.AwaitConnection(cctx); err != nil {
		t.Fatalf("mqtt client await: %v", err)
	}
	t.Cleanup(func() {
		dctx, dcancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer dcancel()
		_ = cm.Disconnect(dctx)
	})
	return cm
}

func (e *testEnv) publishMQTT(t *testing.T, cm *autopaho.ConnectionManager, topic string, qos byte, retain bool, payload string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := cm.Publish(ctx, &paho.Publish{
		QoS:     qos,
		Topic:   topic,
		Retain:  retain,
		Payload: []byte(payload),
	}); err != nil {
		t.Fatalf("mqtt publish %q: %v", topic, err)
	}
}

// safeMsgs is a goroutine-safe message collector.
type safeMsgs struct {
	mu   sync.Mutex
	msgs []*paho.Publish
}

func (s *safeMsgs) add(p *paho.Publish) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.msgs = append(s.msgs, p)
}

func (s *safeMsgs) snapshot() []*paho.Publish {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*paho.Publish(nil), s.msgs...)
}

func eventually(t *testing.T, timeout time.Duration, fn func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal(msg)
}

// --- Tests ---

func TestMQTTToNATS_QoS0_Live(t *testing.T) {
	env := newTestEnv(t)
	sub, err := env.nc.SubscribeSync("home.room.temp")
	if err != nil {
		t.Fatal(err)
	}

	pub := env.mqttClient(t, "pub0", nil)
	env.publishMQTT(t, pub, "home/room/temp", 0, false, "21.5")

	msg, err := sub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("no NATS message: %v", err)
	}
	if string(msg.Data) != "21.5" {
		t.Errorf("payload = %q, want 21.5", msg.Data)
	}
	if msg.Header.Get(markerKey) == "" {
		t.Error("bridged NATS message missing loop marker")
	}
}

func TestMQTTToNATS_QoS1_Durable(t *testing.T) {
	env := newTestEnv(t)
	pub := env.mqttClient(t, "pub1", nil)
	env.publishMQTT(t, pub, "sensors/a", 1, false, "hello")

	stream, err := env.js.Stream(context.Background(), msgsStreamName)
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, 5*time.Second, func() bool {
		m, err := stream.GetLastMsgForSubject(context.Background(), msgsSubjectPrefix+"sensors.a")
		return err == nil && string(m.Data) == "hello"
	}, "QoS1 message not durably stored in msgs stream")
}

func TestMQTTToNATS_QoS2(t *testing.T) {
	env := newTestEnv(t)
	pub := env.mqttClient(t, "pub2", nil)
	env.publishMQTT(t, pub, "q2/topic", 2, false, "once")

	stream, err := env.js.Stream(context.Background(), msgsStreamName)
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, 5*time.Second, func() bool {
		m, err := stream.GetLastMsgForSubject(context.Background(), msgsSubjectPrefix+"q2.topic")
		return err == nil && string(m.Data) == "once"
	}, "QoS2 message not stored")

	// Exactly one message on the subject (no duplication on the happy path).
	info, err := stream.Info(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Msgs != 1 {
		t.Errorf("msgs stream has %d messages, want 1", info.State.Msgs)
	}
}

func TestRetained(t *testing.T) {
	env := newTestEnv(t)
	pub := env.mqttClient(t, "pubR", nil)

	env.publishMQTT(t, pub, "config/x", 1, true, "v1")
	stream, err := env.js.Stream(context.Background(), rmsgsStreamName)
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, 5*time.Second, func() bool {
		m, err := stream.GetLastMsgForSubject(context.Background(), rmsgsSubjectPrefix+"config.x")
		return err == nil && string(m.Data) == "v1"
	}, "retained value not stored")

	// Empty retained payload deletes it.
	env.publishMQTT(t, pub, "config/x", 1, true, "")
	eventually(t, 5*time.Second, func() bool {
		_, err := stream.GetLastMsgForSubject(context.Background(), rmsgsSubjectPrefix+"config.x")
		return err != nil
	}, "retained value not deleted on empty payload")
}

func TestNATSToMQTT(t *testing.T) {
	env := newTestEnv(t)
	got := &safeMsgs{}
	sub := env.mqttClient(t, "subN", got.add)
	subscribeMQTT(t, sub, "#")

	if err := env.nc.Publish("sensors.x", []byte("from-nats")); err != nil {
		t.Fatal(err)
	}

	eventually(t, 5*time.Second, func() bool {
		for _, m := range got.snapshot() {
			if m.Topic == "sensors/x" && string(m.Payload) == "from-nats" {
				if m.Properties == nil || m.Properties.User.Get(markerKey) == "" {
					t.Error("forwarded MQTT message missing loop marker")
				}
				return true
			}
		}
		return false
	}, "NATS message not forwarded to MQTT")
}

func TestNATSToMQTT_SkipsSystemSubjects(t *testing.T) {
	env := newTestEnv(t)
	got := &safeMsgs{}
	sub := env.mqttClient(t, "subSys", got.add)
	subscribeMQTT(t, sub, "#")

	// Publish to an _INBOX subject (NATS protocol traffic) — must NOT forward.
	if err := env.nc.Publish("_INBOX.should.not.bridge", []byte("nope")); err != nil {
		t.Fatal(err)
	}
	// And a normal subject as a positive control / ordering barrier.
	if err := env.nc.Publish("ok.subject", []byte("yes")); err != nil {
		t.Fatal(err)
	}

	eventually(t, 5*time.Second, func() bool {
		for _, m := range got.snapshot() {
			if m.Topic == "ok/subject" {
				return true
			}
		}
		return false
	}, "control message not forwarded")

	for _, m := range got.snapshot() {
		if m.Topic == "_INBOX/should/not/bridge" {
			t.Error("system _INBOX subject was forwarded to MQTT")
		}
	}
}

func TestNoLoopIntegration(t *testing.T) {
	env := newTestEnv(t)
	sub, err := env.nc.SubscribeSync("loop.test")
	if err != nil {
		t.Fatal(err)
	}
	pub := env.mqttClient(t, "pubLoop", nil)
	env.publishMQTT(t, pub, "loop/test", 0, false, "single")

	if _, err := sub.NextMsg(5 * time.Second); err != nil {
		t.Fatalf("expected one message: %v", err)
	}
	// No further copies should arrive (the marker + NoEcho prevent re-bridging).
	if msg, err := sub.NextMsg(500 * time.Millisecond); err == nil {
		t.Errorf("unexpected duplicate NATS message: %q", msg.Data)
	}
}

func subscribeMQTT(t *testing.T, cm *autopaho.ConnectionManager, filter string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := cm.Subscribe(ctx, &paho.Subscribe{
		Subscriptions: []paho.SubscribeOptions{{Topic: filter, QoS: 1}},
	}); err != nil {
		t.Fatalf("mqtt subscribe %q: %v", filter, err)
	}
}
