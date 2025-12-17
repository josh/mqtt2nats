package main

import (
	"context"
	"flag"
	"log/slog"
	"net"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/nats-io/nats.go"
	"tailscale.com/tsnet"
)

const (
	defaultMQTTHostname = "mqtt2nats-mqtt"
	defaultNATSHostname = "mqtt2nats-nats"
)

func main() {
	ctx := context.Background()
	var mqttTsnetEnabled bool
	var mqttBroker string
	var mqttTsAuthKey string
	var mqttTsHostname string
	var natsTsnetEnabled bool
	var natsTsAuthKey string
	var natsTsHostname string
	var natsURL string

	flag.StringVar(&mqttBroker, "mqtt", "", "MQTT Broker URL (required)")
	flag.BoolVar(&mqttTsnetEnabled, "mqtt-tsnet", false, "Use tsnet for MQTT connection")
	flag.StringVar(&mqttTsAuthKey, "mqtt-ts-authkey", "", "Tailscale auth key for MQTT connection")
	flag.StringVar(&mqttTsHostname, "mqtt-ts-hostname", defaultMQTTHostname, "Tailscale hostname for MQTT connection")
	flag.StringVar(&natsURL, "nats", "", "NATS Server URL (required)")
	flag.BoolVar(&natsTsnetEnabled, "nats-tsnet", false, "Use tsnet for NATS connection")
	flag.StringVar(&natsTsAuthKey, "nats-ts-authkey", "", "Tailscale auth key for NATS connection")
	flag.StringVar(&natsTsHostname, "nats-ts-hostname", defaultNATSHostname, "Tailscale hostname for NATS connection")
	flag.Parse()

	if mqttBroker == "" || natsURL == "" {
		flag.Usage()
		os.Exit(1)
	}

	if !mqttTsnetEnabled && (mqttTsAuthKey != "" || mqttTsHostname != defaultMQTTHostname) {
		slog.Error("MQTT tsnet flags provided without -mqtt-tsnet enabled")
		os.Exit(1)
	}

	if !natsTsnetEnabled && (natsTsAuthKey != "" || natsTsHostname != defaultNATSHostname) {
		slog.Error("NATS tsnet flags provided without -nats-tsnet enabled")
		os.Exit(1)
	}

	var stateDir string
	if mqttTsnetEnabled || natsTsnetEnabled {
		stateDir = os.Getenv("XDG_STATE_HOME")
		if stateDir == "" {
			homeDir, err := os.UserHomeDir()
			if err != nil {
				slog.Error("Error getting user home dir", "error", err)
				os.Exit(1)
			}
			stateDir = filepath.Join(homeDir, ".local", "state")
		}
	}

	var natsOpts []nats.Option
	var natsTs *tsnet.Server

	if natsTsnetEnabled {
		natsTs = &tsnet.Server{
			Hostname: natsTsHostname,
			AuthKey:  natsTsAuthKey,
			Dir:      filepath.Join(stateDir, "mqtt2nats", "nats"),
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

	nc, err := nats.Connect(natsURL, natsOpts...)
	if err != nil {
		slog.Error("Error connecting to NATS", "error", err)
		os.Exit(1)
	}
	defer nc.Close()
	slog.Info("Connected to NATS", "url", natsURL)

	opts := mqtt.NewClientOptions()
	opts.AddBroker(mqttBroker)
	opts.SetClientID("mqtt2nats-relay")

	if mqttTsnetEnabled {
		mqttTs := &tsnet.Server{
			Hostname: mqttTsHostname,
			AuthKey:  mqttTsAuthKey,
			Dir:      filepath.Join(stateDir, "mqtt2nats", "mqtt"),
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

		opts.SetCustomOpenConnectionFn(func(uri *url.URL, options mqtt.ClientOptions) (net.Conn, error) {
			return dialWithRetry(ctx, mqttTs, "tcp", uri.Host)
		})
	}

	opts.SetOnConnectHandler(func(client mqtt.Client) {
		slog.Info("Connected to MQTT", "broker", mqttBroker)
	})
	opts.SetConnectionLostHandler(func(client mqtt.Client, err error) {
		slog.Error("Connection lost to MQTT", "error", err)
	})

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		slog.Error("Error connecting to MQTT", "error", token.Error())
		os.Exit(1)
	}

	token := client.Subscribe("#", 0, func(client mqtt.Client, msg mqtt.Message) {
		topic := msg.Topic()
		payload := msg.Payload()
		natsSubject := transformTopic(topic)

		slog.Info("Relaying", "mqtt_topic", topic, "nats_subject", natsSubject)

		if err := nc.Publish(natsSubject, payload); err != nil {
			slog.Error("Error publishing to NATS", "error", err)
		}
	})

	if token.Wait() && token.Error() != nil {
		slog.Error("Error subscribing to MQTT topic", "topic", "#", "error", token.Error())
		os.Exit(1)
	}
	slog.Info("Subscribed to MQTT topic", "topic", "#")

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c

	slog.Info("Shutting down...")
	client.Disconnect(250)
	slog.Info("Exited.")
}

type TailscaleDialer struct {
	srv *tsnet.Server
}

func (d *TailscaleDialer) Dial(network, address string) (net.Conn, error) {
	return dialWithRetry(context.Background(), d.srv, network, address)
}

func dialWithRetry(ctx context.Context, srv *tsnet.Server, network, address string) (net.Conn, error) {
	var conn net.Conn
	var err error

	for attempt := range 10 {
		dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		conn, err = srv.Dial(dialCtx, network, address)
		cancel()

		if err == nil {
			return conn, nil
		}

		if attempt < 9 {
			slog.Warn("Dial failed, retrying", "attempt", attempt+1, "error", err)
			time.Sleep(500 * time.Millisecond)
		}
	}

	return nil, err
}

func transformTopic(topic string) string {
	parts := strings.Split(topic, "/")
	newParts := make([]string, len(parts))
	for i, part := range parts {
		if part == "" {
			newParts[i] = "/"
		} else {
			newParts[i] = strings.ReplaceAll(part, ".", "//")
		}
	}
	return strings.Join(newParts, ".")
}
