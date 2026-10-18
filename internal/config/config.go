// Package config is the only place that reads configuration.
package config

import (
	"fmt"
	"os"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"
)

type Config struct {
	// HealthPort serves GET /health for liveness checks.
	HealthPort string `env:"HEALTH_PORT,required"`
	// ShutdownGrace is how long work already started may continue after a stop signal.
	ShutdownGrace time.Duration `env:"SHUTDOWN_GRACE,required"`
	// WorkerID names this pod in claims (Integrator.Api stores it as claim_owner).
	// Defaults to the host name, which is the pod name in Kubernetes.
	WorkerID string `env:"WORKER_ID"`

	Integrator IntegratorConfig
	Loop       LoopConfig
}

type IntegratorConfig struct {
	// URL of Integrator.Api, e.g. http://localhost:8080.
	URL     string        `env:"INTEGRATOR_URL,required"`
	Timeout time.Duration `env:"INTEGRATOR_TIMEOUT,required"`
	// A call that cannot reach the API is tried again Retries times, RetryDelay apart.
	Retries    int           `env:"INTEGRATOR_RETRIES,required"`
	RetryDelay time.Duration `env:"INTEGRATOR_RETRY_DELAY,required"`
}

type LoopConfig struct {
	// After a round without work a loop waits IdleDelay, doubling up to MaxIdleDelay.
	IdleDelay    time.Duration `env:"LOOP_IDLE_DELAY,required"`
	MaxIdleDelay time.Duration `env:"LOOP_MAX_IDLE_DELAY,required"`
}

// Load reads the env file into the process environment, then parses the
// environment into Config. Variables already set in the environment win.
func Load(path string) (Config, error) {
	if err := godotenv.Load(path); err != nil {
		return Config{}, fmt.Errorf("load config file %s: %w", path, err)
	}
	cfg, err := env.ParseAs[Config]()
	if err != nil {
		return Config{}, fmt.Errorf("parse config: %w", err)
	}
	if cfg.WorkerID == "" {
		if cfg.WorkerID, err = os.Hostname(); err != nil {
			return Config{}, fmt.Errorf("worker id: %w", err)
		}
	}
	return cfg, nil
}
