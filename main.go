package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

const version = "0.0.4"

func main() {
	if len(os.Args) > 1 && os.Args[1] == "version" {
		fmt.Println(version)
		return
	}

	configFlag := flag.String("config", "", "Path to JSON config file (overrides MQTT2NATS_CONFIG and ./mqtt2nats.json)")
	printConfig := flag.Bool("print-config", false, "Print the merged effective config and exit")
	versionFlag := flag.Bool("version", false, "Print version and exit")
	verbose := flag.Bool("verbose", false, "Enable debug-level logging")
	flag.Parse()

	if *versionFlag {
		fmt.Println(version)
		return
	}

	path := resolveConfigPath(*configFlag)
	cfg, err := LoadConfig(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, "mqtt2nats:", err)
		os.Exit(1)
	}

	if *printConfig {
		out, _ := json.MarshalIndent(cfg, "", "  ")
		fmt.Println(string(out))
		return
	}

	if err := cfg.Validate(); err != nil {
		fmt.Fprintln(os.Stderr, "mqtt2nats:", err)
		os.Exit(1)
	}

	level := slog.LevelInfo
	if *verbose {
		level = slog.LevelDebug
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	log.Info("starting mqtt2nats", "version", version, "config", path, "level", level)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := run(ctx, cfg, log); err != nil {
		log.Error("exit", "err", err)
		os.Exit(1)
	}
	log.Info("shutdown complete")
}

func run(ctx context.Context, cfg Config, log *slog.Logger) error {
	b := NewBridge(cfg, log)
	return b.Run(ctx)
}
