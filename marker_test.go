package main

import (
	"testing"

	"github.com/eclipse/paho.golang/paho"
	"github.com/nats-io/nats.go"
)

func TestNATSMarker(t *testing.T) {
	m := nats.NewMsg("home.room.temp")
	if natsIsBridged(m) {
		t.Fatal("fresh NATS message reported as bridged")
	}
	natsSetMarker(m)
	if !natsIsBridged(m) {
		t.Fatal("marked NATS message not reported as bridged")
	}
}

func TestMQTTMarker(t *testing.T) {
	p := &paho.Publish{Topic: "home/room/temp"}
	if mqttIsBridged(p) {
		t.Fatal("fresh MQTT publish reported as bridged")
	}
	props := &paho.PublishProperties{}
	mqttSetMarker(props)
	p.Properties = props
	if !mqttIsBridged(p) {
		t.Fatal("marked MQTT publish not reported as bridged")
	}
}

// TestNoLoop verifies the core invariant: a message tagged by one direction's
// emit path is dropped by the other direction's receive path.
func TestNoLoop(t *testing.T) {
	// MQTT->NATS emits a marked NATS message; NATS->MQTT must drop it.
	out := nats.NewMsg("sensors.x")
	natsSetMarker(out)
	if !natsIsBridged(out) {
		t.Fatal("emitted NATS message should be detected as bridged on receipt")
	}

	// NATS->MQTT emits a marked MQTT publish; MQTT->NATS must drop it.
	props := &paho.PublishProperties{}
	mqttSetMarker(props)
	echoed := &paho.Publish{Topic: "sensors/x", Properties: props}
	if !mqttIsBridged(echoed) {
		t.Fatal("emitted MQTT publish should be detected as bridged on receipt")
	}
}
