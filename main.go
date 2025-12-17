package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/nats-io/nats.go"
	"tailscale.com/tsnet"
)

const (
	defaultMQTTHostname = "mqtt2nats-mqtt"
	defaultNATSHostname = "mqtt2nats-nats"
)

type Config struct {
	mqttBroker        *url.URL
	mqttTsnetEnabled  bool
	mqttTsAuthKey     string
	mqttTsHostname    string
	natsURL           string
	natsTsnetEnabled  bool
	natsTsAuthKey     string
	natsTsHostname    string
	natsSubjectPrefix string
	stateDir          string
	verbose           bool
}

func parseFlags() (*Config, error) {
	cfg := &Config{}

	var mqttBroker string
	var stateDir string
	flag.StringVar(&mqttBroker, "mqtt", "", "MQTT Broker URL (required)")
	flag.BoolVar(&cfg.mqttTsnetEnabled, "mqtt-tsnet", false, "Use tsnet for MQTT connection")
	flag.StringVar(&cfg.mqttTsAuthKey, "mqtt-ts-authkey", "", "Tailscale auth key for MQTT connection")
	flag.StringVar(&cfg.mqttTsHostname, "mqtt-ts-hostname", defaultMQTTHostname, "Tailscale hostname for MQTT connection")
	flag.StringVar(&cfg.natsURL, "nats", "", "NATS Server URL (required)")
	flag.BoolVar(&cfg.natsTsnetEnabled, "nats-tsnet", false, "Use tsnet for NATS connection")
	flag.StringVar(&cfg.natsTsAuthKey, "nats-ts-authkey", "", "Tailscale auth key for NATS connection")
	flag.StringVar(&cfg.natsTsHostname, "nats-ts-hostname", defaultNATSHostname, "Tailscale hostname for NATS connection")
	flag.StringVar(&cfg.natsSubjectPrefix, "nats-subject-prefix", "", "NATS subject prefix (optional)")
	flag.StringVar(&stateDir, "state-dir", "", "State directory for tsnet (optional, defaults to XDG_STATE_HOME or ~/.local/state)")
	flag.BoolVar(&cfg.verbose, "verbose", false, "Enable debug logging")
	flag.Parse()

	if mqttBroker == "" || cfg.natsURL == "" {
		flag.Usage()
		return nil, flag.ErrHelp
	}

	mqttBrokerURL, err := url.Parse(mqttBroker)
	if err != nil {
		return nil, err
	}
	cfg.mqttBroker = mqttBrokerURL

	if !cfg.mqttTsnetEnabled && (cfg.mqttTsAuthKey != "" || cfg.mqttTsHostname != defaultMQTTHostname) {
		return nil, errors.New("MQTT tsnet flags provided without -mqtt-tsnet enabled")
	}

	if !cfg.natsTsnetEnabled && (cfg.natsTsAuthKey != "" || cfg.natsTsHostname != defaultNATSHostname) {
		return nil, errors.New("NATS tsnet flags provided without -nats-tsnet enabled")
	}

	if cfg.mqttTsnetEnabled || cfg.natsTsnetEnabled {
		if stateDir == "" {
			stateDir = os.Getenv("XDG_STATE_HOME")
			if stateDir == "" {
				homeDir, err := os.UserHomeDir()
				if err != nil {
					return nil, fmt.Errorf("error getting user home dir: %w", err)
				}
				stateDir = filepath.Join(homeDir, ".local", "state")
			}
		}
		cfg.stateDir = stateDir
	}

	return cfg, nil
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := parseFlags()
	if err != nil {
		if err == flag.ErrHelp {
			flag.Usage()
		} else {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			flag.Usage()
		}
		os.Exit(1)
	}

	if cfg.verbose {
		slog.SetLogLoggerLevel(slog.LevelDebug)
	}

	var natsTs *tsnet.Server
	var nc *nats.Conn

	natsOpts := []nats.Option{
		nats.NoEcho(),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			slog.Info("NATS reconnected", "url", cfg.natsURL)
		}),
	}

	if cfg.natsTsnetEnabled {
		natsTs = &tsnet.Server{
			Hostname: cfg.natsTsHostname,
			AuthKey:  cfg.natsTsAuthKey,
			Dir:      filepath.Join(cfg.stateDir, "mqtt2nats", "nats"),
		}
		defer func() {
			if err := natsTs.Close(); err != nil {
				slog.Error("Error closing NATS tsnet server", "error", err)
			}
		}()

		if err := natsTs.Start(); err != nil {
			slog.Error("Error starting NATS tsnet server", "error", err)
			os.Exit(1)
		}

		natsOpts = append(natsOpts, nats.SetCustomDialer(&TailscaleDialer{srv: natsTs}))
	}

	var mqttTs *tsnet.Server
	if cfg.mqttTsnetEnabled {
		mqttTs = &tsnet.Server{
			Hostname: cfg.mqttTsHostname,
			AuthKey:  cfg.mqttTsAuthKey,
			Dir:      filepath.Join(cfg.stateDir, "mqtt2nats", "mqtt"),
		}
		defer func() {
			if err := mqttTs.Close(); err != nil {
				slog.Error("Error closing MQTT tsnet server", "error", err)
			}
		}()

		if err := mqttTs.Start(); err != nil {
			slog.Error("Error starting MQTT tsnet server", "error", err)
			os.Exit(1)
		}
	}

	clientConfig := paho.ClientConfig{
		ClientID: "mqtt2nats-relay",
		Router:   paho.NewStandardRouter(),
		OnPublishReceived: []func(paho.PublishReceived) (bool, error){
			func(pr paho.PublishReceived) (bool, error) {
				topic := pr.Packet.Topic
				payload := pr.Packet.Payload
				natsSubject := transformTopic(topic, cfg.natsSubjectPrefix)

				slog.Debug("Relaying message", "mqtt_topic", topic, "nats_subject", natsSubject, "payload_size", len(payload))

				if err := nc.Publish(natsSubject, payload); err != nil {
					slog.Error("Error publishing to NATS", "error", err, "nats_subject", natsSubject)
				}
				return true, nil
			},
		},
		OnClientError: func(err error) {
			slog.Error("MQTT client error", "error", err)
		},
		OnServerDisconnect: func(d *paho.Disconnect) {
			if d.Properties != nil && d.Properties.ReasonString != "" {
				slog.Warn("Server requested disconnect", "reason", d.Properties.ReasonString, "reason_code", d.ReasonCode)
			} else {
				slog.Warn("Server requested disconnect", "reason_code", d.ReasonCode)
			}
		},
	}

	autopahoConfig := autopaho.ClientConfig{
		ServerUrls:            []*url.URL{cfg.mqttBroker},
		KeepAlive:             60,
		ConnectTimeout:        30 * time.Second,
		ConnectRetryDelay:     0,
		SessionExpiryInterval: 300,
		OnConnectionUp: func(cm *autopaho.ConnectionManager, connAck *paho.Connack) {
			slog.Info("MQTT connection established", "broker", cfg.mqttBroker.String())
			if connAck.Properties != nil {
				if connAck.Properties.ServerKeepAlive != nil {
					slog.Info("Server keepalive", "keepalive", *connAck.Properties.ServerKeepAlive)
				}
				if connAck.Properties.SessionExpiryInterval != nil {
					slog.Info("Session expiry interval", "interval", *connAck.Properties.SessionExpiryInterval)
				}
			}
			_, err := cm.Subscribe(ctx, &paho.Subscribe{
				Subscriptions: []paho.SubscribeOptions{
					{Topic: "#", QoS: 0, NoLocal: true},
				},
			})
			if err != nil {
				slog.Error("Failed to subscribe to MQTT topic", "topic", "#", "error", err)
			} else {
				slog.Info("Subscribed to MQTT topic", "topic", "#")
			}
		},
		OnConnectionDown: func() bool {
			slog.Warn("MQTT connection lost, will reconnect")
			return true
		},
		OnConnectError: func(err error) {
			slog.Error("Error whilst attempting MQTT connection", "error", err)
		},
		ClientConfig: clientConfig,
	}

	if cfg.mqttTsnetEnabled {
		autopahoConfig.AttemptConnection = func(ctx context.Context, acfg autopaho.ClientConfig, uri *url.URL) (net.Conn, error) {
			return mqttTs.Dial(ctx, "tcp", uri.Host)
		}
	}

	nc, err = nats.Connect(cfg.natsURL, natsOpts...)
	if err != nil {
		slog.Error("Error connecting to NATS", "error", err)
		os.Exit(1)
	}
	defer nc.Close()
	slog.Info("Connected to NATS", "url", cfg.natsURL)

	cm, err := autopaho.NewConnection(ctx, autopahoConfig)
	if err != nil {
		slog.Error("Error creating MQTT connection manager", "error", err)
		os.Exit(1)
	}

	if err := cm.AwaitConnection(ctx); err != nil {
		slog.Error("Error awaiting MQTT connection", "error", err)
		os.Exit(1)
	}

	slog.Info("MQTT connection manager started, waiting for shutdown signal")
	<-ctx.Done()

	slog.Info("Shutdown signal received, disconnecting...")
	if err := cm.Disconnect(ctx); err != nil {
		slog.Error("Error disconnecting from MQTT", "error", err)
	}
	<-cm.Done()
	slog.Info("Shutdown complete")
}

type TailscaleDialer struct {
	srv *tsnet.Server
}

func (d *TailscaleDialer) Dial(network, address string) (net.Conn, error) {
	return d.srv.Dial(context.Background(), network, address)
}

func transformTopic(topic, prefix string) string {
	parts := strings.Split(topic, "/")
	newParts := make([]string, len(parts))
	for i, part := range parts {
		if part == "" {
			newParts[i] = "/"
		} else {
			newParts[i] = strings.ReplaceAll(part, ".", "//")
		}
	}
	subject := strings.Join(newParts, ".")
	if prefix != "" {
		return prefix + "." + subject
	}
	return subject
}
