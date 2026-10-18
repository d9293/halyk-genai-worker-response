package poll

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/d9293/halyk-genai-worker-response/internal/client/ai"
	"github.com/d9293/halyk-genai-worker-response/internal/client/integrator"
)

var testConfig = Config{BatchSize: 4, Workers: 2}

func claimed(attemptID string) integrator.ClaimedPoll {
	return integrator.ClaimedPoll{
		AttemptID: attemptID, AttemptNo: 1, RequestGUID: "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
		Operation: "REGISTER_COMPARE", ClaimToken: "token", ClaimUntil: time.Now().Add(30 * time.Second),
	}
}

// fixture wires the loop to fakes: AI says processing and Integrator.Api agrees, unless a test changes them.
type fixture struct {
	loop *Loop
	api  *integratorClientMock
	ai   *aiClientMock
}

func newFixture(polls ...integrator.ClaimedPoll) *fixture {
	f := &fixture{}
	f.api = &integratorClientMock{
		ClaimPollsFunc:        func(context.Context, int) ([]integrator.ClaimedPoll, error) { return polls, nil },
		ReportObservationFunc: func(context.Context, integrator.ClaimedPoll, integrator.Observation) error { return nil },
	}
	f.ai = &aiClientMock{
		StatusFunc: func(context.Context, string, string) ai.Status { return ai.Status{Outcome: ai.OutcomeProcessing} },
	}
	f.loop = NewLoop(testConfig, f.api, f.ai)
	return f
}

func TestStep_ChecksAndReports(t *testing.T) {
	f := newFixture(claimed("a1"))

	didWork, err := f.loop.Step(context.Background())
	if err != nil || !didWork {
		t.Fatalf("didWork=%v err=%v", didWork, err)
	}

	asked := f.ai.StatusCalls()
	if len(asked) != 1 || asked[0].Operation != "REGISTER_COMPARE" || asked[0].TaskID != "a1" {
		t.Fatalf("asked AI %+v; the task_id is the attempt ID", asked)
	}
	reports := f.api.ReportObservationCalls()
	if len(reports) != 1 || reports[0].P.ClaimToken != "token" {
		t.Fatalf("reports = %+v", reports)
	}
	if got := reports[0].O; got.Outcome != "PROCESSING" || got.Result != nil || got.Failure != nil {
		t.Errorf("observation = %+v", got)
	}
}

func TestStep_ObservationCarriesResultOrFailure(t *testing.T) {
	tests := []struct {
		name   string
		status ai.Status
		want   integrator.Observation
	}{
		{"succeeded", ai.Status{Outcome: ai.OutcomeSucceeded, Result: json.RawMessage(`{"status":"done"}`)},
			integrator.Observation{Outcome: "SUCCEEDED", Result: json.RawMessage(`{"status":"done"}`)}},
		{"failed", ai.Status{Outcome: ai.OutcomeFailed, FailureCode: ai.CodeAITaskFailed, FailureMessage: "Нет доступа"},
			integrator.Observation{Outcome: "FAILED", Failure: &integrator.Failure{Code: "AI_TASK_FAILED", Message: "Нет доступа"}}},
		{"technical error", ai.Status{Outcome: ai.OutcomeTechnicalError, FailureCode: ai.CodeAITimeout, FailureMessage: "slow"},
			integrator.Observation{Outcome: "TECHNICAL_ERROR", Failure: &integrator.Failure{Code: "AI_TIMEOUT", Message: "slow"}}},
		{"not found", ai.Status{Outcome: ai.OutcomeNotFound}, integrator.Observation{Outcome: "NOT_FOUND"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(claimed("a1"))
			f.ai.StatusFunc = func(context.Context, string, string) ai.Status { return tt.status }

			_, _ = f.loop.Step(context.Background())

			got := f.api.ReportObservationCalls()[0].O
			if got.Outcome != tt.want.Outcome || string(got.Result) != string(tt.want.Result) ||
				(got.Failure == nil) != (tt.want.Failure == nil) || (got.Failure != nil && *got.Failure != *tt.want.Failure) {
				t.Errorf("observation = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestStep_NothingClaimed(t *testing.T) {
	f := newFixture()
	if didWork, err := f.loop.Step(context.Background()); didWork || err != nil {
		t.Errorf("didWork=%v err=%v", didWork, err)
	}
	if len(f.ai.StatusCalls()) != 0 {
		t.Error("AI must not be called")
	}
}

func TestStep_ClaimError(t *testing.T) {
	f := newFixture()
	f.api.ClaimPollsFunc = func(context.Context, int) ([]integrator.ClaimedPoll, error) { return nil, integrator.ErrUnavailable }
	if _, err := f.loop.Step(context.Background()); !errors.Is(err, integrator.ErrUnavailable) {
		t.Errorf("err = %v", err)
	}
}

func TestStep_FailedReportDoesNotStopOthers(t *testing.T) {
	f := newFixture(claimed("a1"), claimed("a2"), claimed("a3"))
	f.api.ReportObservationFunc = func(_ context.Context, p integrator.ClaimedPoll, _ integrator.Observation) error {
		if p.AttemptID == "a1" {
			return integrator.ErrClaimLost
		}
		return nil
	}
	if didWork, err := f.loop.Step(context.Background()); !didWork || err != nil {
		t.Fatalf("didWork=%v err=%v", didWork, err)
	}
	if n := len(f.api.ReportObservationCalls()); n != 3 {
		t.Errorf("reported %d of 3", n)
	}
}

func TestStep_NoReportOnShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	f := newFixture(claimed("a1"))
	f.ai.StatusFunc = func(context.Context, string, string) ai.Status {
		cancel() // grace period is over while the GET is in flight
		return ai.Status{Outcome: ai.OutcomeTechnicalError, FailureCode: ai.CodeAIUnavailable}
	}

	_, _ = f.loop.Step(ctx)

	if len(f.api.ReportObservationCalls()) != 0 {
		t.Error("an answer cut off by shutdown must not be reported")
	}
}

func TestStep_RunsAtMostWorkersAtOnce(t *testing.T) {
	f := newFixture(claimed("a1"), claimed("a2"), claimed("a3"), claimed("a4"))
	var running, maxSeen atomic.Int32
	f.ai.StatusFunc = func(context.Context, string, string) ai.Status {
		n := running.Add(1)
		for m := maxSeen.Load(); n > m && !maxSeen.CompareAndSwap(m, n); m = maxSeen.Load() {
		}
		time.Sleep(20 * time.Millisecond)
		running.Add(-1)
		return ai.Status{Outcome: ai.OutcomeQueued}
	}

	_, _ = f.loop.Step(context.Background())

	if len(f.api.ReportObservationCalls()) != 4 {
		t.Fatal("every claimed task must be checked and reported")
	}
	if int(maxSeen.Load()) > testConfig.Workers {
		t.Errorf("%d checks ran at once, limit %d", maxSeen.Load(), testConfig.Workers)
	}
}
