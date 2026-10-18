// Package app wires worker.Response and runs it until shutdown.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/d9293/halyk-genai-worker-response/internal/broker/kafka"
	"github.com/d9293/halyk-genai-worker-response/internal/client/ai"
	"github.com/d9293/halyk-genai-worker-response/internal/client/integrator"
	"github.com/d9293/halyk-genai-worker-response/internal/config"
	"github.com/d9293/halyk-genai-worker-response/internal/runner"
	"github.com/d9293/halyk-genai-worker-response/internal/usecase/poll"
	"github.com/d9293/halyk-genai-worker-response/internal/usecase/result"
)

// loop is one independent worker loop, e.g. polling AI or publishing results.
type loop struct {
	name string
	step runner.Step
}

// Run starts the loops and the health endpoint, and on ctx cancellation shuts
// down in order: stop claiming new work, let work in progress finish for up to
// ShutdownGrace, then cancel what is left.
func Run(ctx context.Context, cfg config.Config) error {
	api := integrator.New(integrator.Config{
		BaseURL:    cfg.Integrator.URL,
		Timeout:    cfg.Integrator.Timeout,
		Retries:    cfg.Integrator.Retries,
		RetryDelay: cfg.Integrator.RetryDelay,
		WorkerID:   cfg.WorkerID,
	})

	aiClient, err := ai.New(ai.Config{
		BaseURLs: map[string]string{
			"REGISTER_COMPARE": cfg.AI.RegisterCompareURL,
			"SOCIAL_MEDIA":     cfg.AI.SocialMediaURL,
		},
		Timeout:          cfg.AI.Timeout,
		MaxResponseBytes: cfg.AI.MaxResponseBytes,
	})
	if err != nil {
		return err
	}
	polls := poll.NewLoop(poll.Config{BatchSize: cfg.Polls.BatchSize, Workers: cfg.Polls.Workers}, api, aiClient)

	producer, err := kafka.New(kafka.Config{
		Brokers:  cfg.Kafka.Brokers,
		Version:  cfg.Kafka.Version,
		ClientID: cfg.Kafka.ClientID,
		Timeout:  cfg.Kafka.Timeout,
	})
	if err != nil {
		return err
	}
	// Closed after the loops are done, so publishes in progress can finish.
	defer func() {
		if err := producer.Close(); err != nil {
			slog.Warn("close kafka producer", "error", err)
		}
	}()
	results := result.NewLoop(result.Config{BatchSize: cfg.Results.BatchSize, Workers: cfg.Results.Workers}, api, producer)

	loops := []loop{
		{name: "polls", step: polls.Step},
		{name: "results", step: results.Step},
	}

	health := &http.Server{
		Addr:              ":" + cfg.HealthPort,
		Handler:           healthHandler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	healthErr := make(chan error, 1)
	go func() { healthErr <- health.ListenAndServe() }()

	work, cancelWork := context.WithCancel(context.Background())
	defer cancelWork()

	var wg sync.WaitGroup
	for _, l := range loops {
		wg.Go(func() {
			runner.Run(ctx, work, l.name, runner.Config{IdleDelay: cfg.Loop.IdleDelay, MaxIdleDelay: cfg.Loop.MaxIdleDelay}, l.step)
		})
	}

	slog.Info("worker.Response started",
		"workerId", cfg.WorkerID,
		"integrator", cfg.Integrator.URL,
		"kafka", cfg.Kafka.Brokers,
		"healthPort", cfg.HealthPort,
		"loops", len(loops),
	)

	select {
	case err := <-healthErr:
		return fmt.Errorf("health server: %w", err)
	case <-ctx.Done():
	}

	slog.Info("stopping: no new work is claimed; waiting for work in progress", "grace", cfg.ShutdownGrace)
	if !waitTimeout(&wg, cfg.ShutdownGrace) {
		slog.Warn("grace period over, cancelling work in progress")
		cancelWork()
		wg.Wait()
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := health.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("shutdown health server: %w", err)
	}
	slog.Info("worker.Response stopped")
	return nil
}

func healthHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	return mux
}

// waitTimeout waits for wg, giving up after d. It reports whether wg finished.
func waitTimeout(wg *sync.WaitGroup, d time.Duration) bool {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(d):
		return false
	}
}
