// worker.Response: checks on tasks at AI and delivers finished results to BPM
// via Kafka, for the BPM ↔ AI integration. All state lives in Integrator.Api.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/d9293/halyk-genai-worker-response/internal/app"
	"github.com/d9293/halyk-genai-worker-response/internal/config"
)

func main() {
	configFile := flag.String("config_file", "", "path to the env config file, e.g. dev.env (required)")
	flag.Parse()
	if *configFile == "" {
		fmt.Fprintln(os.Stderr, "--config_file is required, e.g. go run . --config_file dev.env")
		os.Exit(2)
	}

	cfg, err := config.Load(*configFile)
	if err != nil {
		slog.Error("load config", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := app.Run(ctx, cfg); err != nil {
		slog.Error("worker stopped", "error", err)
		os.Exit(1)
	}
}
