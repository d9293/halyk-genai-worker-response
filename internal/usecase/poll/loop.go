// Package poll asks AI how tasks are doing and reports each answer to
// Integrator.Api, which decides what follows.
package poll

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/d9293/halyk-genai-worker-response/internal/client/ai"
	"github.com/d9293/halyk-genai-worker-response/internal/client/integrator"
)

//go:generate go run github.com/matryer/moq@v0.8.1 -out mocks_test.go . integratorClient aiClient

type integratorClient interface {
	ClaimPolls(ctx context.Context, limit int) ([]integrator.ClaimedPoll, error)
	ReportObservation(ctx context.Context, p integrator.ClaimedPoll, o integrator.Observation) error
}

type aiClient interface {
	Status(ctx context.Context, operation, taskID string) ai.Status
}

type Config struct {
	// BatchSize is how many tasks one round claims.
	BatchSize int
	// Workers is how many tasks are checked at the same time.
	Workers int
}

type Loop struct {
	cfg Config
	api integratorClient
	ai  aiClient
}

func NewLoop(cfg Config, api integratorClient, ai aiClient) *Loop {
	return &Loop{cfg: cfg, api: api, ai: ai}
}

// Step claims a batch of status checks and does them, at most Workers at once.
// It reports whether there was any work.
func (l *Loop) Step(ctx context.Context) (bool, error) {
	polls, err := l.api.ClaimPolls(ctx, l.cfg.BatchSize)
	if err != nil {
		return false, fmt.Errorf("claim polls: %w", err)
	}
	if len(polls) == 0 {
		return false, nil
	}

	slots := make(chan struct{}, l.cfg.Workers)
	var wg sync.WaitGroup
	for _, p := range polls {
		slots <- struct{}{}
		wg.Go(func() {
			defer func() { <-slots }()
			l.process(ctx, p)
		})
	}
	wg.Wait()
	return true, nil
}

// process checks one task and reports what AI said. Errors are logged, not
// returned: one bad task must not stop the others.
func (l *Loop) process(ctx context.Context, p integrator.ClaimedPoll) {
	log := slog.With("attemptId", p.AttemptID, "attemptNo", p.AttemptNo, "requestGuid", p.RequestGUID, "operation", p.Operation)

	status := l.ai.Status(ctx, p.Operation, p.AttemptID)
	if ctx.Err() != nil {
		// Shutting down: the answer is not trustworthy and asking is harmless,
		// so report nothing. The claim expires and the task is checked again.
		log.Info("shutdown during the status check; leaving the task to Integrator.Api")
		return
	}

	err := l.api.ReportObservation(ctx, p, toObservation(status))
	switch {
	case errors.Is(err, integrator.ErrClaimLost):
		log.Warn("claim lost before the observation was reported", "outcome", status.Outcome)
	case err != nil:
		log.Warn("could not report the observation", "outcome", status.Outcome, "error", err)
	case status.FailureCode != "":
		log.Warn("task checked", "outcome", status.Outcome, "code", status.FailureCode, "message", status.FailureMessage)
	default:
		log.Info("task checked", "outcome", status.Outcome)
	}
}

func toObservation(s ai.Status) integrator.Observation {
	o := integrator.Observation{Outcome: s.Outcome, Result: s.Result}
	if s.FailureCode != "" {
		o.Failure = &integrator.Failure{Code: s.FailureCode, Message: s.FailureMessage}
	}
	return o
}
