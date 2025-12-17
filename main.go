package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
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
	"sync"
	"syscall"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/nats-io/nats.go"
	"tailscale.com/tsnet"
)

type Config struct {
	mqttBroker        *url.URL
	mqttClientID      string
	mqttTsnetEnabled  bool
	mqttTsAuthKey     string
	mqttTsHostname    string
	natsURL           *url.URL
	natsTsnetEnabled  bool
	natsTsAuthKey     string
	natsTsHostname    string
	natsSubjectPrefix string
	stateDir          string
	verbose           bool
}

const (
	defaultHostname = "mqtt2nats"
)

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

	var wg sync.WaitGroup

	nc, err := startNatsConnection(ctx, cfg, &wg)
	if err != nil {
		slog.Error("Error connecting to NATS", "error", err)
		os.Exit(1)
	}
	slog.Info("Connected to NATS", "url", cfg.natsURL)

	cm, err := startMqttConnection(ctx, cfg, &wg, func(pr paho.PublishReceived) (bool, error) {
		topic := pr.Packet.Topic
		payload := pr.Packet.Payload
		natsSubject := mqttTopicToNatsSubject(topic, cfg.natsSubjectPrefix)

		slog.Debug("Relaying message MQTT -> NATS", "mqtt_topic", topic, "nats_subject", natsSubject, "payload_size", len(payload))

		if err := nc.Publish(natsSubject, payload); err != nil {
			slog.Error("Error publishing to NATS", "error", err, "nats_subject", natsSubject)
		}
		return true, nil
	})
	if err != nil {
		slog.Error("Error connecting to MQTT", "error", err)
		os.Exit(1)
	}
	slog.Info("Connected to MQTT", "broker", cfg.mqttBroker)

	if err := relayNatsToMqtt(ctx, cfg, nc, cm, &wg); err != nil {
		slog.Error("Error relaying NATS to MQTT", "error", err)
		os.Exit(1)
	}

	wg.Wait()
	slog.Debug("All cleanup handlers finished")
}

func startNatsConnection(ctx context.Context, cfg *Config, wg *sync.WaitGroup) (*nats.Conn, error) {
	var nc *nats.Conn
	var natsTs *tsnet.Server

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		slog.Debug("Shutting down NATS connection...")

		if nc != nil {
			nc.Close()
		}

		if natsTs != nil {
			if err := natsTs.Close(); err != nil {
				slog.Error("Error closing NATS tsnet server", "error", err)
			}
		}

		slog.Debug("NATS connection shut down")
	}()

	natsOpts := []nats.Option{
		nats.NoEcho(),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second),
		nats.ReconnectHandler(func(conn *nats.Conn) {
			slog.Info("NATS reconnected", "url", cfg.natsURL)
		}),
	}

	if cfg.natsTsnetEnabled {
		natsTs = &tsnet.Server{
			Hostname: cfg.natsTsHostname,
			AuthKey:  cfg.natsTsAuthKey,
			Dir:      filepath.Join(cfg.stateDir, "mqtt2nats", "nats"),
		}

		if err := natsTs.Start(); err != nil {
			return nil, fmt.Errorf("error starting NATS tsnet server: %w", err)
		}
		slog.Info("NATS tsnet server started", "hostname", cfg.natsTsHostname)

		natsOpts = append(natsOpts, nats.SetCustomDialer(&TailscaleDialer{srv: natsTs}))
	}

	nc, err := nats.Connect(cfg.natsURL.String(), natsOpts...)
	if err != nil {
		return nil, fmt.Errorf("error connecting to NATS: %w", err)
	}

	return nc, nil
}

func startMqttConnection(ctx context.Context, cfg *Config, wg *sync.WaitGroup, onPublishReceived func(paho.PublishReceived) (bool, error)) (*autopaho.ConnectionManager, error) {
	var mqttTs *tsnet.Server
	var cm *autopaho.ConnectionManager

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		slog.Debug("Shutting down MQTT connection...")

		if cm != nil {
			if err := cm.Disconnect(ctx); err != nil {
				slog.Error("Error disconnecting from MQTT", "error", err)
			}
		}

		if mqttTs != nil {
			if err := mqttTs.Close(); err != nil {
				slog.Error("Error closing MQTT tsnet server", "error", err)
			}
		}

		slog.Debug("MQTT connection shut down")
	}()

	if cfg.mqttTsnetEnabled {
		mqttTs = &tsnet.Server{
			Hostname: cfg.mqttTsHostname,
			AuthKey:  cfg.mqttTsAuthKey,
			Dir:      filepath.Join(cfg.stateDir, "mqtt2nats", "mqtt"),
		}

		if err := mqttTs.Start(); err != nil {
			return nil, fmt.Errorf("error starting MQTT tsnet server: %w", err)
		}
		slog.Info("MQTT tsnet server started", "hostname", cfg.mqttTsHostname)
	}

	slog.Debug("Using MQTT client ID", "client_id", cfg.mqttClientID)

	clientConfig := paho.ClientConfig{
		ClientID:          cfg.mqttClientID,
		OnPublishReceived: []func(paho.PublishReceived) (bool, error){onPublishReceived},
		OnClientError: func(err error) {
			slog.Error("MQTT client error", "error", err)
		},
		OnServerDisconnect: func(d *paho.Disconnect) {
			slog.Warn("MQTT server disconnected", "reason_code", d.ReasonCode)
			slog.Debug("MQTT server disconnected", "properties", d.Properties)
		},
	}

	autopahoConfig := autopaho.ClientConfig{
		ServerUrls: []*url.URL{cfg.mqttBroker},
		OnConnectionUp: func(cm *autopaho.ConnectionManager, connAck *paho.Connack) {
			slog.Info("MQTT connection established", "broker", cfg.mqttBroker)

			_, err := cm.Subscribe(ctx, &paho.Subscribe{
				Subscriptions: []paho.SubscribeOptions{
					{Topic: "#", QoS: 0, NoLocal: true},
				},
			})
			if err != nil {
				slog.Error("Failed to subscribe to MQTT topic", "topic", "#", "error", err)
			}
		},
		OnConnectionDown: func() bool {
			slog.Warn("MQTT connection down, will reconnect")
			return true
		},
		OnConnectError: func(err error) {
			slog.Error("MQTT connection error", "error", err)
		},
		ClientConfig: clientConfig,
	}

	if cfg.mqttTsnetEnabled {
		autopahoConfig.AttemptConnection = func(ctx context.Context, acfg autopaho.ClientConfig, uri *url.URL) (net.Conn, error) {
			slog.Debug("Dialing MQTT via Tailscale", "uri", uri.Host)
			return mqttTs.Dial(ctx, "tcp", uri.Host)
		}
	}

	cm, err := autopaho.NewConnection(ctx, autopahoConfig)
	if err != nil {
		return nil, fmt.Errorf("error creating MQTT connection manager: %w", err)
	}

	return cm, nil
}

func relayNatsToMqtt(ctx context.Context, cfg *Config, nc *nats.Conn, cm *autopaho.ConnectionManager, wg *sync.WaitGroup) error {
	var natsSub *nats.Subscription

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		slog.Debug("Shutting down NATS to MQTT relay...")

		if natsSub != nil {
			if err := natsSub.Unsubscribe(); err != nil {
				slog.Error("Error unsubscribing from NATS", "error", err)
			}
		}
		slog.Debug("NATS to MQTT relay shut down")
	}()

	var subjectPattern string
	if cfg.natsSubjectPrefix != "" {
		subjectPattern = cfg.natsSubjectPrefix + ".>"
	} else {
		subjectPattern = ">"
	}

	natsSub, err := nc.Subscribe(subjectPattern, func(msg *nats.Msg) {
		natsSubject := msg.Subject
		payload := msg.Data
		mqttTopic := natsSubjectToMqttTopic(natsSubject, cfg.natsSubjectPrefix)

		slog.Debug("Relaying message NATS -> MQTT", "nats_subject", natsSubject, "mqtt_topic", mqttTopic, "payload_size", len(payload))

		_, err := cm.Publish(ctx, &paho.Publish{
			Topic:   mqttTopic,
			Payload: payload,
			QoS:     0,
		})
		if err != nil {
			slog.Error("Error publishing to MQTT", "error", err, "mqtt_topic", mqttTopic)
		}
	})

	if err != nil {
		return fmt.Errorf("error subscribing to NATS: %w", err)
	}

	slog.Debug("Subscribed to NATS", "subject", subjectPattern)

	return nil
}

func parseFlags() (*Config, error) {
	cfg := &Config{}

	var mqttBroker string
	var natsURL string
	var stateDir string
	var mqttClientID string
	flag.StringVar(&mqttBroker, "mqtt", "", "MQTT Broker URL (required)")
	flag.StringVar(&mqttClientID, "mqtt-client-id", "", "MQTT client ID (optional, defaults to random)")
	flag.BoolVar(&cfg.mqttTsnetEnabled, "mqtt-tsnet", false, "Use tsnet for MQTT connection")
	flag.StringVar(&cfg.mqttTsAuthKey, "mqtt-ts-authkey", "", "Tailscale auth key for MQTT connection")
	flag.StringVar(&cfg.mqttTsHostname, "mqtt-ts-hostname", defaultHostname, "Tailscale hostname for MQTT connection")
	flag.StringVar(&natsURL, "nats", "", "NATS Server URL (required)")
	flag.BoolVar(&cfg.natsTsnetEnabled, "nats-tsnet", false, "Use tsnet for NATS connection")
	flag.StringVar(&cfg.natsTsAuthKey, "nats-ts-authkey", "", "Tailscale auth key for NATS connection")
	flag.StringVar(&cfg.natsTsHostname, "nats-ts-hostname", defaultHostname, "Tailscale hostname for NATS connection")
	flag.StringVar(&cfg.natsSubjectPrefix, "nats-subject-prefix", "", "NATS subject prefix (optional)")
	flag.StringVar(&stateDir, "state-dir", "", "State directory for tsnet (optional, defaults to XDG_STATE_HOME or ~/.local/state)")
	flag.BoolVar(&cfg.verbose, "verbose", false, "Enable debug logging")
	flag.Parse()

	if mqttBroker == "" || natsURL == "" {
		flag.Usage()
		return nil, flag.ErrHelp
	}

	mqttBrokerURL, err := url.Parse(mqttBroker)
	if err != nil {
		return nil, err
	}
	cfg.mqttBroker = mqttBrokerURL

	natsURLParsed, err := url.Parse(natsURL)
	if err != nil {
		return nil, err
	}
	cfg.natsURL = natsURLParsed

	if !cfg.mqttTsnetEnabled && (cfg.mqttTsAuthKey != "" || cfg.mqttTsHostname != defaultHostname) {
		return nil, errors.New("MQTT tsnet flags provided without -mqtt-tsnet enabled")
	}

	if !cfg.natsTsnetEnabled && (cfg.natsTsAuthKey != "" || cfg.natsTsHostname != defaultHostname) {
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

	if mqttClientID == "" {
		randomID, err := generateRandomClientID()
		if err != nil {
			return nil, fmt.Errorf("error generating random client ID: %w", err)
		}
		cfg.mqttClientID = randomID
	} else {
		cfg.mqttClientID = mqttClientID
	}

	return cfg, nil
}

func generateRandomClientID() (string, error) {
	bytes := make([]byte, 4)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return fmt.Sprintf("mqtt2nats-%s", hex.EncodeToString(bytes)), nil
}

type TailscaleDialer struct {
	srv *tsnet.Server
}

func (d *TailscaleDialer) Dial(network, address string) (net.Conn, error) {
	slog.Debug("Dialing NATS via Tailscale", "network", network, "address", address)
	return d.srv.Dial(context.Background(), network, address)
}

func mqttTopicToNatsSubject(topic, addPrefix string) string {
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
	if addPrefix != "" {
		return addPrefix + "." + subject
	}
	return subject
}

func natsSubjectToMqttTopic(subject, removePrefix string) string {
	if removePrefix != "" && strings.HasPrefix(subject, removePrefix+".") {
		subject = strings.TrimPrefix(subject, removePrefix+".")
	}
	parts := strings.Split(subject, ".")
	newParts := make([]string, len(parts))
	for i, part := range parts {
		if part == "/" {
			newParts[i] = ""
		} else {
			newParts[i] = strings.ReplaceAll(part, "//", ".")
		}
	}
	return strings.Join(newParts, "/")
}
