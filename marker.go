package main

import (
	"github.com/eclipse/paho.golang/paho"
	"github.com/nats-io/nats.go"
)

// The loop marker is a generic "this message has already been relayed by a
// bridge" tag. Both receive paths drop any message that already carries it, so
// a message published onto one side is never bridged back. It is intentionally
// not per-instance, so it also suppresses loops across future replicas.
const (
	markerKey   = "X-Mqtt2Nats-Bridged"
	markerValue = "1"
)

// natsSetMarker tags an outgoing NATS message as bridge-originated.
func natsSetMarker(m *nats.Msg) {
	if m.Header == nil {
		m.Header = nats.Header{}
	}
	m.Header.Set(markerKey, markerValue)
}

// natsIsBridged reports whether a received NATS message already carries the marker.
func natsIsBridged(m *nats.Msg) bool {
	return m.Header.Get(markerKey) != ""
}

// mqttSetMarker tags outgoing MQTT publish properties as bridge-originated.
func mqttSetMarker(props *paho.PublishProperties) {
	props.User.Add(markerKey, markerValue)
}

// mqttIsBridged reports whether a received MQTT publish already carries the marker.
func mqttIsBridged(p *paho.Publish) bool {
	return p.Properties != nil && p.Properties.User.Get(markerKey) != ""
}
