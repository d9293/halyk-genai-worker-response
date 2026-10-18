package result

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/d9293/halyk-genai-worker-response/internal/broker/kafka"
	"github.com/d9293/halyk-genai-worker-response/internal/client/integrator"
)

var testConfig = Config{BatchSize: 4, Workers: 2}

const message = `{"schemaVersion":1,"eventType":"TASK_COMPLETED","requestGuid":"g1","status":"SUCCEEDED"}`

func claimed(requestGUID string) integrator.ClaimedResult {
	return integrator.ClaimedResult{
		RequestGUID: requestGUID, Topic: "integrator.task-results.v1", MessageKey: "r1",
		Message: json.RawMessage(message), ClaimToken: "token", ClaimUntil: time.Now().Add(30 * time.Second),
	}
}

// fixture wires the loop to fakes: Kafka accepts and Integrator.Api agrees, unless a test changes them.
type fixture struct {
	loop  *Loop
	api   *integratorClientMock
	kafka *publisherMock
}

func newFixture(results ...integrator.ClaimedResult) *fixture {
	f := &fixture{}
	f.api = &integratorClientMock{
		ClaimResultsFunc:         func(context.Context, int) ([]integrator.ClaimedResult, error) { return results, nil },
		MarkPublishedFunc:        func(context.Context, integrator.ClaimedResult) error { return nil },
		ReportPublishFailureFunc: func(context.Context, integrator.ClaimedResult, integrator.Failure) error { return nil },
	}
	f.kafka = &publisherMock{
		PublishFunc: func(context.Context, string, string, []byte) error { return nil },
	}
	f.loop = NewLoop(testConfig, f.api, f.kafka)
	return f
}

func TestStep_PublishesAndMarks(t *testing.T) {
	f := newFixture(claimed("g1"))

	didWork, err := f.loop.Step(context.Background())
	if err != nil || !didWork {
		t.Fatalf("didWork=%v err=%v", didWork, err)
	}

	sent := f.kafka.PublishCalls()
	if len(sent) != 1 || sent[0].Topic != "integrator.task-results.v1" || sent[0].Key != "r1" || string(sent[0].Value) != message {
		t.Fatalf("published %+v; the message must go out exactly as claimed", sent)
	}
	marked := f.api.MarkPublishedCalls()
	if len(marked) != 1 || marked[0].R.ClaimToken != "token" || len(f.api.ReportPublishFailureCalls()) != 0 {
		t.Errorf("marked = %+v", marked)
	}
}

func TestStep_ReportsKafkaFailure(t *testing.T) {
	tests := []struct {
		name string
		err  error
		code string
	}{
		{"unavailable", fmt.Errorf("%w: no brokers", kafka.ErrUnavailable), CodeKafkaUnavailable},
		{"too large", fmt.Errorf("%w: 2MB", kafka.ErrTooLarge), CodeKafkaMessageLarge},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(claimed("g1"))
			f.kafka.PublishFunc = func(context.Context, string, string, []byte) error { return tt.err }

			_, _ = f.loop.Step(context.Background())

			failures := f.api.ReportPublishFailureCalls()
			if len(failures) != 1 || failures[0].F.Code != tt.code || failures[0].F.Message != tt.err.Error() {
				t.Fatalf("failures = %+v", failures)
			}
			if len(f.api.MarkPublishedCalls()) != 0 {
				t.Error("a failed publish must not be marked published")
			}
		})
	}
}

func TestStep_NothingClaimed(t *testing.T) {
	f := newFixture()
	if didWork, err := f.loop.Step(context.Background()); didWork || err != nil {
		t.Errorf("didWork=%v err=%v", didWork, err)
	}
	if len(f.kafka.PublishCalls()) != 0 {
		t.Error("Kafka must not be called")
	}
}

func TestStep_ClaimError(t *testing.T) {
	f := newFixture()
	f.api.ClaimResultsFunc = func(context.Context, int) ([]integrator.ClaimedResult, error) { return nil, integrator.ErrUnavailable }
	if _, err := f.loop.Step(context.Background()); !errors.Is(err, integrator.ErrUnavailable) {
		t.Errorf("err = %v", err)
	}
}

func TestStep_MarksEvenWhenShutdownStartsDuringPublish(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	f := newFixture(claimed("g1"))
	f.kafka.PublishFunc = func(context.Context, string, string, []byte) error {
		cancel() // grace period is over while the publish is in flight
		return nil
	}
	var markErr error
	f.api.MarkPublishedFunc = func(ctx context.Context, _ integrator.ClaimedResult) error {
		markErr = ctx.Err()
		return markErr
	}

	_, _ = f.loop.Step(ctx)

	if len(f.api.MarkPublishedCalls()) != 1 || markErr != nil {
		t.Errorf("a published message must be marked even during shutdown (ctx err %v)", markErr)
	}
}

func TestStep_SkipsResultsNotStartedBeforeShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	f := newFixture(claimed("g1"), claimed("g2"))

	_, _ = f.loop.Step(ctx)

	if len(f.kafka.PublishCalls()) != 0 || len(f.api.MarkPublishedCalls()) != 0 || len(f.api.ReportPublishFailureCalls()) != 0 {
		t.Error("nothing may be published or reported once shutdown has begun")
	}
}

func TestStep_LostClaimDoesNotStopOthers(t *testing.T) {
	f := newFixture(claimed("g1"), claimed("g2"), claimed("g3"))
	f.api.MarkPublishedFunc = func(_ context.Context, r integrator.ClaimedResult) error {
		if r.RequestGUID == "g1" {
			return integrator.ErrClaimLost
		}
		return nil
	}
	_, _ = f.loop.Step(context.Background())
	if n := len(f.kafka.PublishCalls()); n != 3 {
		t.Errorf("published %d of 3", n)
	}
}

func TestStep_RunsAtMostWorkersAtOnce(t *testing.T) {
	f := newFixture(claimed("g1"), claimed("g2"), claimed("g3"), claimed("g4"))
	var running, maxSeen atomic.Int32
	f.kafka.PublishFunc = func(context.Context, string, string, []byte) error {
		n := running.Add(1)
		for m := maxSeen.Load(); n > m && !maxSeen.CompareAndSwap(m, n); m = maxSeen.Load() {
		}
		time.Sleep(20 * time.Millisecond)
		running.Add(-1)
		return nil
	}

	_, _ = f.loop.Step(context.Background())

	if len(f.api.MarkPublishedCalls()) != 4 {
		t.Fatal("every claimed result must be published and marked")
	}
	if int(maxSeen.Load()) > testConfig.Workers {
		t.Errorf("%d publishes ran at once, limit %d", maxSeen.Load(), testConfig.Workers)
	}
}
