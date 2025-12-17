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

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/nats-io/nats.go"
	"tailscale.com/tsnet"
)

func main() {
	var mqttBroker string
	var mqttTsAuthKey string
	var mqttTsHostname string
	var natsTsAuthKey string
	var natsTsHostname string
	var natsURL string

	flag.StringVar(&mqttBroker, "mqtt", "", "MQTT Broker URL (required)")
	flag.StringVar(&mqttTsAuthKey, "mqtt-ts-authkey", "", "Tailscale auth key for MQTT connection")
	flag.StringVar(&mqttTsHostname, "mqtt-hostname", "mqtt2nats-mqtt", "Tailscale hostname for MQTT connection")
	flag.StringVar(&natsTsAuthKey, "nats-ts-authkey", "", "Tailscale auth key for NATS connection")
	flag.StringVar(&natsTsHostname, "nats-hostname", "mqtt2nats-nats", "Tailscale hostname for NATS connection")
	flag.StringVar(&natsURL, "nats", "", "NATS Server URL (required)")
	flag.Parse()

	if mqttBroker == "" || natsURL == "" {
		flag.Usage()
		os.Exit(1)
	}

	stateDir := os.Getenv("XDG_STATE_HOME")
	if stateDir == "" {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			slog.Error("Error getting user home dir", "error", err)
			os.Exit(1)
		}
		stateDir = filepath.Join(homeDir, ".local", "state")
	}

	natsTs := new(tsnet.Server)
	natsTs.Hostname = natsTsHostname
	natsTs.AuthKey = natsTsAuthKey
	natsTs.Dir = filepath.Join(stateDir, "mqtt2nats", "nats")
	defer natsTs.Close()

	if err := natsTs.Start(); err != nil {
		slog.Error("Error starting NATS tsnet server", "error", err)
		os.Exit(1)
	}

	mqttTs := new(tsnet.Server)
	mqttTs.Hostname = mqttTsHostname
	mqttTs.AuthKey = mqttTsAuthKey
	mqttTs.Dir = filepath.Join(stateDir, "mqtt2nats", "mqtt")
	defer mqttTs.Close()

	if err := mqttTs.Start(); err != nil {
		slog.Error("Error starting MQTT tsnet server", "error", err)
		os.Exit(1)
	}

	natsOpts := []nats.Option{
		nats.SetCustomDialer(&TailscaleDialer{srv: natsTs}),
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
	opts.SetCustomOpenConnectionFn(func(uri *url.URL, options mqtt.ClientOptions) (net.Conn, error) {
		return mqttTs.Dial(context.Background(), "tcp", uri.Host)
	})
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
	return d.srv.Dial(context.Background(), network, address)
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
