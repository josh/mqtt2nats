package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"
)

// Config is the on-disk JSON configuration. Secrets are referenced by file
// path (mounted from Kubernetes Secrets), never stored inline.
type Config struct {
	MQTT     MQTTConfig `json:"mqtt"`
	NATS     NATSConfig `json:"nats"`
	HTTPAddr string     `json:"http_addr"`
}

// MQTTConfig describes the connection to the external MQTT 5.0 broker.
type MQTTConfig struct {
	BrokerURL    string `json:"broker_url"`
	ClientID     string `json:"client_id"`
	Username     string `json:"username"`
	PasswordFile string `json:"password_file"`
	TopicFilter  string `json:"topic_filter"`
	// SessionExpiry keeps the broker-side session (and in-flight QoS 1/2 state)
	// alive across bridge restarts so the persistent session can resume.
	SessionExpiry Duration `json:"session_expiry"`
	KeepAlive     Duration `json:"keepalive"`
}

// NATSConfig describes the connection to the (exclusive) NATS account. Exactly
// one auth method is used, selected by whichever *File is set.
type NATSConfig struct {
	URL          string `json:"url"`
	CredsFile    string `json:"creds_file"`
	TokenFile    string `json:"token_file"`
	UserFile     string `json:"user_file"`
	PasswordFile string `json:"password_file"`
}

// defaultConfig returns the baseline config that JSON overrides are merged on
// top of, so -print-config shows the effective merged result.
func defaultConfig() Config {
	return Config{
		MQTT: MQTTConfig{
			ClientID:      "mqtt2nats",
			TopicFilter:   "#",
			SessionExpiry: Duration(24 * time.Hour),
			KeepAlive:     Duration(30 * time.Second),
		},
		NATS: NATSConfig{
			URL: "nats://localhost:4222",
		},
		HTTPAddr: ":8080",
	}
}

// resolveConfigPath picks the config file path: the -config flag, then the
// MQTT2NATS_CONFIG env var, then ./mqtt2nats.json if it exists.
func resolveConfigPath(flag string) string {
	if flag != "" {
		return flag
	}
	if env := os.Getenv("MQTT2NATS_CONFIG"); env != "" {
		return env
	}
	if _, err := os.Stat("mqtt2nats.json"); err == nil {
		return "mqtt2nats.json"
	}
	return ""
}

// LoadConfig reads path (if non-empty) over the built-in defaults and validates
// the result. An empty path yields the defaults alone.
func LoadConfig(path string) (Config, error) {
	cfg := defaultConfig()
	if path != "" {
		b, err := os.ReadFile(path)
		if err != nil {
			return Config{}, fmt.Errorf("read config %q: %w", path, err)
		}
		dec := json.NewDecoder(strings.NewReader(string(b)))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&cfg); err != nil {
			return Config{}, fmt.Errorf("parse config %q: %w", path, err)
		}
	}
	return cfg, nil
}

// Validate checks required fields.
func (c Config) Validate() error {
	if c.MQTT.BrokerURL == "" {
		return fmt.Errorf("mqtt.broker_url is required")
	}
	if c.MQTT.ClientID == "" {
		return fmt.Errorf("mqtt.client_id must not be empty")
	}
	if c.NATS.URL == "" {
		return fmt.Errorf("nats.url is required")
	}
	return nil
}

// readSecretFile reads a secret from a file path, trimming surrounding
// whitespace (so trailing newlines in mounted Secrets are ignored).
func readSecretFile(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read secret %q: %w", path, err)
	}
	return strings.TrimSpace(string(b)), nil
}

// Duration is a time.Duration that (un)marshals as a Go duration string such
// as "24h" or "30s" in JSON.
type Duration time.Duration

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

func (d *Duration) UnmarshalJSON(b []byte) error {
	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	switch x := v.(type) {
	case string:
		dur, err := time.ParseDuration(x)
		if err != nil {
			return fmt.Errorf("invalid duration %q: %w", x, err)
		}
		*d = Duration(dur)
	case float64:
		*d = Duration(time.Duration(x))
	default:
		return fmt.Errorf("invalid duration: %v", v)
	}
	return nil
}
