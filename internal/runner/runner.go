// Package runner repeats a unit of work until shutdown: claim something from
// Integrator.Api and process it. When there was nothing to do, or the step
// failed, it waits before trying again, longer each time up to a limit, so an
// idle worker does not hammer the API.
package runner

import (
	"context"
	"log/slog"
	"time"
)

// Step does one round of work, e.g. claim a batch and process it. It reports
// whether there was any work. ctx is cancelled only when the grace period of
// a shutdown is over, so work already started can finish.
type Step func(ctx context.Context) (didWork bool, err error)

type Config struct {
	// IdleDelay is the first wait after a round without work; it doubles up to MaxIdleDelay.
	IdleDelay    time.Duration
	MaxIdleDelay time.Duration
}

// Run calls step until stop is cancelled. A new round never starts after stop
// is cancelled; the round in progress keeps running with work.
func Run(stop, work context.Context, name string, cfg Config, step Step) {
	delay := cfg.IdleDelay
	for stop.Err() == nil {
		didWork, err := step(work)
		switch {
		case err != nil:
			slog.Warn("round failed", "loop", name, "error", err)
		case didWork:
			delay = cfg.IdleDelay
			continue // more work may be waiting: ask again right away
		}

		select {
		case <-stop.Done():
		case <-time.After(delay):
		}
		delay = min(delay*2, cfg.MaxIdleDelay)
	}
	slog.Info("loop stopped", "loop", name)
}
