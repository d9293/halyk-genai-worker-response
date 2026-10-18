package integrator

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// fakeAPI answers every call with the next of answers (the last one repeats)
// and records what it received.
type fakeAPI struct {
	answers []answer
	calls   atomic.Int32
	path    atomic.Value // last request path
	body    atomic.Value // last request body
}

type answer struct {
	status int
	body   string
	delay  time.Duration
}

func (f *fakeAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	n := int(f.calls.Add(1)) - 1
	a := f.answers[min(n, len(f.answers)-1)]
	b, _ := io.ReadAll(r.Body)
	f.path.Store(r.URL.Path)
	f.body.Store(string(b))
	time.Sleep(a.delay)
	w.WriteHeader(a.status)
	_, _ = w.Write([]byte(a.body))
}

func newClient(t *testing.T, answers ...answer) (*Client, *fakeAPI) {
	t.Helper()
	api := &fakeAPI{answers: answers}
	srv := httptest.NewServer(api)
	t.Cleanup(srv.Close)
	return New(Config{
		BaseURL: srv.URL + "/", Timeout: 200 * time.Millisecond,
		Retries: 2, RetryDelay: time.Millisecond, WorkerID: "pod-1",
	}), api
}

func jsonBody(t *testing.T, api *fakeAPI) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal([]byte(api.body.Load().(string)), &m); err != nil {
		t.Fatalf("request body: %v", err)
	}
	return m
}

var res = ClaimedResult{RequestGUID: "g1", ClaimToken: "t1"}

func TestClaimPolls(t *testing.T) {
	c, api := newClient(t, answer{200, `{"items":[{"attemptId":"a1","attemptNo":2,"requestGuid":"g1","requestId":"r1",
		"operation":"SOCIAL_MEDIA","claimToken":"t1","claimUntil":"2026-10-18T10:00:00Z"}]}`, 0})

	polls, err := c.ClaimPolls(context.Background(), 4)
	if err != nil {
		t.Fatal(err)
	}
	if api.path.Load() != "/internal/v1/polls/claim" {
		t.Errorf("path = %v", api.path.Load())
	}
	if b := jsonBody(t, api); b["workerId"] != "pod-1" || b["limit"] != float64(4) {
		t.Errorf("body = %v", b)
	}
	want := ClaimedPoll{
		AttemptID: "a1", AttemptNo: 2, RequestGUID: "g1", RequestID: "r1", Operation: "SOCIAL_MEDIA",
		ClaimToken: "t1", ClaimUntil: time.Date(2026, 10, 18, 10, 0, 0, 0, time.UTC),
	}
	if len(polls) != 1 || polls[0] != want {
		t.Errorf("polls = %+v", polls)
	}
}

func TestReportObservation(t *testing.T) {
	c, api := newClient(t, answer{200, `{"applied":true}`, 0})
	ctx := context.Background()
	p := ClaimedPoll{AttemptID: "a1", ClaimToken: "t1"}

	if err := c.ReportObservation(ctx, p, Observation{Outcome: ObservationProcessing}); err != nil {
		t.Fatal(err)
	}
	if api.path.Load() != "/internal/v1/polls/a1/observation" || api.body.Load() != `{"claimToken":"t1","outcome":"PROCESSING"}` {
		t.Errorf("processing: %v %v", api.path.Load(), api.body.Load())
	}

	succeeded := Observation{Outcome: ObservationSucceeded, Result: json.RawMessage(`{"summary":{"total":3}}`)}
	if err := c.ReportObservation(ctx, p, succeeded); err != nil {
		t.Fatal(err)
	}
	if api.body.Load() != `{"claimToken":"t1","outcome":"SUCCEEDED","result":{"summary":{"total":3}}}` {
		t.Errorf("succeeded: %v", api.body.Load())
	}

	failed := Observation{Outcome: ObservationTechnicalError, Failure: &Failure{Code: "AI_UNAVAILABLE", Message: "504"}}
	if err := c.ReportObservation(ctx, p, failed); err != nil {
		t.Fatal(err)
	}
	if b := jsonBody(t, api); b["result"] != nil || b["failure"].(map[string]any)["code"] != "AI_UNAVAILABLE" {
		t.Errorf("technical error: %v", b)
	}
}

func TestClaimResultsKeepsMessageBytes(t *testing.T) {
	// Odd spacing and key order on purpose: the message must come through byte for byte.
	message := `{"schemaVersion":1,"eventType":"TASK_COMPLETED",  "requestGuid":"g1","result":{"b":1,"a":2.50},"error":null}`
	c, api := newClient(t, answer{200, `{"items":[{"requestGuid":"g1","topic":"integrator.task-results.v1","messageKey":"r1",
		"message":` + message + `,"claimToken":"t1","claimUntil":"2026-10-18T10:00:00Z"}]}`, 0})

	results, err := c.ClaimResults(context.Background(), 2)
	if err != nil || len(results) != 1 {
		t.Fatalf("results = %+v, err %v", results, err)
	}
	r := results[0]
	if r.RequestGUID != "g1" || r.Topic != "integrator.task-results.v1" || r.MessageKey != "r1" || r.ClaimToken != "t1" {
		t.Errorf("result = %+v", r)
	}
	if string(r.Message) != message {
		t.Errorf("message changed:\n got %s\nwant %s", r.Message, message)
	}
	if api.path.Load() != "/internal/v1/results/claim" {
		t.Errorf("path = %v", api.path.Load())
	}
}

func TestResultReports(t *testing.T) {
	c, api := newClient(t, answer{200, `{"applied":true}`, 0})
	ctx := context.Background()

	if err := c.MarkPublished(ctx, res); err != nil {
		t.Fatal(err)
	}
	if api.path.Load() != "/internal/v1/results/g1/published" || api.body.Load() != `{"claimToken":"t1"}` {
		t.Errorf("published: %v %v", api.path.Load(), api.body.Load())
	}

	if err := c.ReportPublishFailure(ctx, res, Failure{Code: "KAFKA_UNAVAILABLE", Message: "no brokers"}); err != nil {
		t.Fatal(err)
	}
	if api.path.Load() != "/internal/v1/results/g1/failure" ||
		api.body.Load() != `{"claimToken":"t1","failure":{"code":"KAFKA_UNAVAILABLE","message":"no brokers"}}` {
		t.Errorf("failure: %v %v", api.path.Load(), api.body.Load())
	}
}

func TestErrors(t *testing.T) {
	tests := []struct {
		name      string
		answers   []answer
		wantErr   error
		wantCalls int32
	}{
		{"claim lost: not retried", []answer{{409, `{"error":{"code":"CLAIM_LOST","message":"x"}}`, 0}}, ErrClaimLost, 1},
		{"not found: not retried", []answer{{404, `{"error":{"code":"RESULT_NOT_FOUND","message":"x"}}`, 0}}, ErrNotFound, 1},
		{"503 then OK: retried", []answer{{503, `{"error":{"code":"SERVICE_UNAVAILABLE"}}`, 0}, {200, `{"applied":true}`, 0}}, nil, 2},
		{"always 503: gives up", []answer{{503, ``, 0}}, ErrUnavailable, 3},
		{"timeout: retried, gives up", []answer{{200, ``, time.Second}}, ErrUnavailable, 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, api := newClient(t, tt.answers...)
			err := c.MarkPublished(context.Background(), res)
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("err = %v, want %v", err, tt.wantErr)
			}
			if n := api.calls.Load(); n != tt.wantCalls {
				t.Errorf("calls = %d, want %d", n, tt.wantCalls)
			}
		})
	}
}

func TestBadRequestIsNotRetried(t *testing.T) {
	c, api := newClient(t, answer{400, `{"error":{"code":"VALIDATION_ERROR","message":"Некорректный запрос"}}`, 0})

	err := c.MarkPublished(context.Background(), res)
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Status != 400 || apiErr.Code != "VALIDATION_ERROR" {
		t.Fatalf("err = %v, want an APIError 400", err)
	}
	if n := api.calls.Load(); n != 1 {
		t.Errorf("calls = %d, want 1", n)
	}
}

func TestShutdownStopsRetrying(t *testing.T) {
	c, api := newClient(t, answer{503, ``, 0})
	c.cfg.RetryDelay = time.Hour
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	err := c.MarkPublished(ctx, res)
	if !errors.Is(err, context.Canceled) || api.calls.Load() != 1 {
		t.Errorf("err = %v, calls %d; want cancellation after the first call", err, api.calls.Load())
	}
}
