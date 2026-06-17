package main

import (
	"net/http"
	"time"
)

// newHealthServer returns an HTTP server exposing Kubernetes probes:
//   - /healthz (liveness): 200 while the process is alive — independent of
//     broker connectivity, so an outage doesn't get the pod killed.
//   - /readyz (readiness): 200 only when both connections are up, else 503.
func newHealthServer(addr string, ready func() bool) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if ready() {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ready\n"))
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("not ready\n"))
	})
	return &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
}
