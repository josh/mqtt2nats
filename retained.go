package main

import (
	"context"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// storeRetained upserts (or, for an empty payload, deletes) the retained
// last-value for a subject in the rmsgs stream (MaxMsgsPerSubject:1 keeps only
// the latest). Best-effort: failures are logged but do not block acking the
// primary publish, which has already landed in NATS.
func (b *Bridge) storeRetained(ctx context.Context, subject string, payload []byte) {
	rsubj := rmsgsSubjectPrefix + subject

	if len(payload) == 0 {
		// An empty retained payload clears the retained message [MQTT-3.3.1-11].
		if err := b.rmsgs.Purge(ctx, jetstream.WithPurgeSubject(rsubj)); err != nil {
			b.log.Warn("retained delete failed", "subject", subject, "err", err)
		}
		return
	}

	rm := nats.NewMsg(rsubj)
	rm.Data = payload
	natsSetMarker(rm)
	if _, err := b.js.PublishMsg(ctx, rm); err != nil {
		b.log.Warn("retained store failed", "subject", subject, "err", err)
	}
}
