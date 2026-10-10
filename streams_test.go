package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/url"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func startNATSCluster(t *testing.T) string {
	t.Helper()
	ports := make([]int, 3)
	routes := make([]*url.URL, 3)
	for i := range ports {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		ports[i] = ln.Addr().(*net.TCPAddr).Port
		routes[i] = &url.URL{Scheme: "nats", Host: ln.Addr().String()}
		_ = ln.Close()
	}

	var servers []*natsserver.Server
	for i, port := range ports {
		ns, err := natsserver.NewServer(&natsserver.Options{
			ServerName: fmt.Sprintf("n%d", i),
			Host:       "127.0.0.1",
			Port:       -1,
			JetStream:  true,
			StoreDir:   t.TempDir(),
			Cluster: natsserver.ClusterOpts{
				Name: "test",
				Host: "127.0.0.1",
				Port: port,
			},
			Routes: routes,
			NoLog:  true,
			NoSigs: true,
		})
		if err != nil {
			t.Fatalf("new nats server: %v", err)
		}
		go ns.Start()
		t.Cleanup(func() {
			ns.Shutdown()
			ns.WaitForShutdown()
		})
		servers = append(servers, ns)
	}
	for _, ns := range servers {
		if !ns.ReadyForConnections(20 * time.Second) {
			t.Fatal("nats not ready")
		}
	}
	eventually(t, 20*time.Second, func() bool {
		for _, ns := range servers {
			if ns.JetStreamIsLeader() {
				return true
			}
		}
		return false
	}, "jetstream meta leader not elected")
	return servers[0].ClientURL()
}

func TestEnsureStreams_ScalesReplicas(t *testing.T) {
	natsURL := startNATSCluster(t)
	nc, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	b := NewBridge(Config{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	b.ctx = ctx
	b.js = js

	assertReplicas := func(want int) {
		t.Helper()
		for _, name := range []string{msgsStreamName, rmsgsStreamName} {
			eventually(t, 20*time.Second, func() bool {
				s, err := js.Stream(ctx, name)
				if err != nil {
					return false
				}
				info := s.CachedInfo()
				return info.Config.Replicas == want && info.Cluster != nil && len(info.Cluster.Replicas) == want-1
			}, fmt.Sprintf("%s did not reach %d replicas", name, want))
		}
	}

	eventually(t, 20*time.Second, func() bool { return b.ensureStreams(ctx) == nil }, "streams not created")
	assertReplicas(1)

	b.cfg.NATS.StreamReplicas = 3
	if err := b.ensureStreams(ctx); err != nil {
		t.Fatal(err)
	}
	assertReplicas(3)
}
