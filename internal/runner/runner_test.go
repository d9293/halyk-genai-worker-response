package runner

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

var fast = Config{IdleDelay: 10 * time.Millisecond, MaxIdleDelay: 40 * time.Millisecond}

func TestRun_WorkRunsBackToBack(t *testing.T) {
	stop, cancel := context.WithCancel(context.Background())
	var rounds atomic.Int32
	go func() {
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()

	Run(stop, context.Background(), "test", Config{IdleDelay: time.Hour, MaxIdleDelay: time.Hour},
		func(context.Context) (bool, error) {
			rounds.Add(1)
			time.Sleep(time.Millisecond)
			return true, nil
		})

	// With work every round there is no idle wait, so many rounds fit in 30ms.
	if n := rounds.Load(); n < 5 {
		t.Errorf("rounds = %d, want many: work must not wait for the idle delay", n)
	}
}

func TestRun_IdleBacksOff(t *testing.T) {
	stop, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	var rounds atomic.Int32

	Run(stop, context.Background(), "test", fast, func(context.Context) (bool, error) {
		rounds.Add(1)
		return false, nil
	})

	// Waits 10, 20, 40, 40, ... ms: about 5 rounds in 150ms, not 15.
	if n := rounds.Load(); n < 3 || n > 7 {
		t.Errorf("rounds = %d, want about 5 with backoff", n)
	}
}

func TestRun_ErrorsWaitAndKeepGoing(t *testing.T) {
	stop, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	var rounds atomic.Int32

	Run(stop, context.Background(), "test", fast, func(context.Context) (bool, error) {
		rounds.Add(1)
		return false, errors.New("integrator unavailable")
	})

	if n := rounds.Load(); n < 2 {
		t.Errorf("rounds = %d: a failing round must be retried after a wait", n)
	}
}

func TestRun_StopLetsTheCurrentRoundFinish(t *testing.T) {
	stop, cancel := context.WithCancel(context.Background())
	var finished atomic.Bool
	started := make(chan struct{})
	go func() {
		<-started
		cancel()
	}()

	done := make(chan struct{})
	go func() {
		Run(stop, context.Background(), "test", fast, func(work context.Context) (bool, error) {
			if finished.Load() {
				t.Error("a new round started after stop")
			}
			close(started)
			time.Sleep(30 * time.Millisecond) // still copying when stop arrives
			if work.Err() != nil {
				t.Error("the work context must stay alive during the grace period")
			}
			finished.Store(true)
			return true, nil
		})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return after stop")
	}
	if !finished.Load() {
		t.Error("the round in progress was not allowed to finish")
	}
}
