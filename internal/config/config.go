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
	Polls      PollsConfig
	AI         AIConfig
	Results    ResultsConfig
	Kafka      KafkaConfig
}

// ResultsConfig controls publishing finished tasks to Kafka.
type ResultsConfig struct {
	// BatchSize results are claimed per round; Workers of them are published at once.
	BatchSize int `env:"RESULT_BATCH_SIZE,required"`
	Workers   int `env:"RESULT_WORKERS,required"`
}

// KafkaConfig is where results go. The topic comes from Integrator.Api with each result.
type KafkaConfig struct {
	// Brokers, comma-separated, e.g. localhost:29092.
	Brokers []string `env:"KAFKA_BROKERS,required"`
	// Version of the brokers, e.g. 3.9.0; needed for idempotent writes.
	Version  string `env:"KAFKA_VERSION,required"`
	ClientID string `env:"KAFKA_CLIENT_ID,required"`
	// Timeout of one publish. Keep it well under the result claim lease (30s).
	Timeout time.Duration `env:"KAFKA_TIMEOUT,required"`
}

// PollsConfig controls checking on tasks at AI.
type PollsConfig struct {
	// BatchSize checks are claimed per round; Workers of them run at once.
	BatchSize int `env:"POLL_BATCH_SIZE,required"`
	Workers   int `env:"POLL_WORKERS,required"`
}

// AIConfig is where the AI team's services are, one base URL per operation.
type AIConfig struct {
	RegisterCompareURL string `env:"AI_REGISTER_COMPARE_URL,required"`
	SocialMediaURL     string `env:"AI_SOCIAL_MEDIA_URL,required"`
	// Timeout of one status GET. Keep it well under the poll claim lease (30s).
	Timeout time.Duration `env:"AI_TIMEOUT,required"`
	// MaxResponseBytes is the biggest AI answer passed on to Integrator.Api,
	// which accepts request bodies up to 1 MiB.
	MaxResponseBytes int64 `env:"AI_MAX_RESPONSE_BYTES,required"`
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
