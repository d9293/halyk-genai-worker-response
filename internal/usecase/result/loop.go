// Package result publishes finished tasks' messages to Kafka for BPM and
// reports each publish to Integrator.Api.
package result

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/d9293/halyk-genai-worker-response/internal/broker/kafka"
	"github.com/d9293/halyk-genai-worker-response/internal/client/integrator"
)

//go:generate go run github.com/matryer/moq@v0.5.3 -out mocks_test.go . integratorClient publisher

type integratorClient interface {
	ClaimResults(ctx context.Context, limit int) ([]integrator.ClaimedResult, error)
	MarkPublished(ctx context.Context, r integrator.ClaimedResult) error
	ReportPublishFailure(ctx context.Context, r integrator.ClaimedResult, f integrator.Failure) error
}

type publisher interface {
	Publish(ctx context.Context, topic, key string, value []byte) error
}

// Failure codes reported when Kafka does not take a message.
const (
	CodeKafkaUnavailable  = "KAFKA_UNAVAILABLE"
	CodeKafkaMessageLarge = "KAFKA_MESSAGE_TOO_LARGE"
)

// reportTimeout bounds reporting a publish, which also runs during shutdown.
const reportTimeout = 10 * time.Second

const maxMessageLen = 2000

type Config struct {
	// BatchSize is how many results one round claims.
	BatchSize int
	// Workers is how many results are published at the same time.
	Workers int
}

type Loop struct {
	cfg   Config
	api   integratorClient
	kafka publisher
}

func NewLoop(cfg Config, api integratorClient, kafka publisher) *Loop {
	return &Loop{cfg: cfg, api: api, kafka: kafka}
}

// Step claims a batch of results and publishes them, at most Workers at once.
// It reports whether there was any work.
func (l *Loop) Step(ctx context.Context) (bool, error) {
	results, err := l.api.ClaimResults(ctx, l.cfg.BatchSize)
	if err != nil {
		return false, fmt.Errorf("claim results: %w", err)
	}
	if len(results) == 0 {
		return false, nil
	}

	slots := make(chan struct{}, l.cfg.Workers)
	var wg sync.WaitGroup
	for _, r := range results {
		slots <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-slots }()
			l.process(ctx, r)
		}()
	}
	wg.Wait()
	return true, nil
}

// process publishes one result and reports the outcome. Errors are logged,
// not returned: one bad result must not stop the others.
func (l *Loop) process(ctx context.Context, r integrator.ClaimedResult) {
	log := slog.With("requestGuid", r.RequestGUID, "topic", r.Topic, "messageKey", r.MessageKey)

	if ctx.Err() != nil {
		// Shutting down before publishing: nothing was sent. The claim expires
		// and the result is handed out again.
		log.Info("shutdown before publishing; leaving the result to Integrator.Api")
		return
	}
	pubErr := l.kafka.Publish(ctx, r.Topic, r.MessageKey, r.Message)

	// Report even during shutdown: a published message that is not marked
	// would be published again. BPM ignores the repeat (by requestGuid), but
	// there is no reason to cause it.
	reportCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), reportTimeout)
	defer cancel()

	if pubErr != nil {
		failure := integrator.Failure{Code: CodeKafkaUnavailable, Message: truncate(pubErr.Error())}
		if errors.Is(pubErr, kafka.ErrTooLarge) {
			failure.Code = CodeKafkaMessageLarge
		}
		log.Warn("publish failed", "code", failure.Code, "error", pubErr)
		if err := l.api.ReportPublishFailure(reportCtx, r, failure); err != nil {
			log.Warn("could not report the failed publish", "error", err)
		}
		return
	}

	err := l.api.MarkPublished(reportCtx, r)
	switch {
	case errors.Is(err, integrator.ErrClaimLost):
		log.Warn("claim lost before the publish was reported; the result may be published again")
	case err != nil:
		log.Warn("could not report the publish; the result may be published again", "error", err)
	default:
		log.Info("result published")
	}
}

func truncate(s string) string {
	if r := []rune(s); len(r) > maxMessageLen {
		return string(r[:maxMessageLen])
	}
	return s
}
