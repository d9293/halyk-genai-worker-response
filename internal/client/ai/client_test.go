package ai

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type fakeAI struct {
	status int
	body   string
	delay  time.Duration
	path   atomic.Value
}

func (f *fakeAI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.path.Store(r.Method + " " + r.URL.Path)
	time.Sleep(f.delay)
	w.WriteHeader(f.status)
	_, _ = w.Write([]byte(f.body))
}

func newClient(t *testing.T, ai *fakeAI) *Client {
	t.Helper()
	srv := httptest.NewServer(ai)
	t.Cleanup(srv.Close)
	c, err := New(Config{
		BaseURLs:         map[string]string{"REGISTER_COMPARE": srv.URL + "/register-compare/", "SOCIAL_MEDIA": srv.URL + "/social-media"},
		Timeout:          100 * time.Millisecond,
		MaxResponseBytes: 1000,
	})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestPaths(t *testing.T) {
	ai := &fakeAI{status: 200, body: `{"task_id":"a1","status":"queued"}`}
	c := newClient(t, ai)

	c.Status(context.Background(), "REGISTER_COMPARE", "a1")
	if ai.path.Load() != "GET /register-compare/api/v1/tasks/a1" {
		t.Errorf("register-compare path = %v", ai.path.Load())
	}
	c.Status(context.Background(), "SOCIAL_MEDIA", "a1")
	if ai.path.Load() != "GET /social-media/api/v1/tasks/a1" {
		t.Errorf("social-media path = %v", ai.path.Load())
	}
}

func TestAnswers(t *testing.T) {
	tests := []struct {
		name    string
		ai      *fakeAI
		outcome string
		code    string
	}{
		{"queued", &fakeAI{status: 200, body: `{"task_id":"a1","status":"queued","summary":null}`}, OutcomeQueued, ""},
		{"processing", &fakeAI{status: 200, body: `{"task_id":"a1","status":"processing"}`}, OutcomeProcessing, ""},
		{"error", &fakeAI{status: 200, body: `{"task_id":"a1","status":"error","error":"Нет доступа к файлу"}`}, OutcomeFailed, CodeAITaskFailed},
		{"AI's task not found", &fakeAI{status: 404, body: `{"detail":"Задача не найдена: a1"}`}, OutcomeNotFound, ""},
		{"route not found", &fakeAI{status: 404, body: `{"detail":"Not Found"}`}, OutcomeTechnicalError, CodeAIUnavailable},
		{"404 from a proxy", &fakeAI{status: 404, body: `<html>404 Not Found</html>`}, OutcomeTechnicalError, CodeAIUnavailable},
		{"504", &fakeAI{status: 504, body: `Gateway Timeout`}, OutcomeTechnicalError, CodeAIUnavailable},
		{"500", &fakeAI{status: 500, body: `{"detail":"boom"}`}, OutcomeTechnicalError, CodeAIUnavailable},
		{"timeout", &fakeAI{status: 200, body: `{"task_id":"a1","status":"queued"}`, delay: time.Second}, OutcomeTechnicalError, CodeAITimeout},
		{"unreadable 200", &fakeAI{status: 200, body: `ok`}, OutcomeTechnicalError, CodeAIBadResponse},
		{"unknown status", &fakeAI{status: 200, body: `{"task_id":"a1","status":"paused"}`}, OutcomeTechnicalError, CodeAIBadResponse},
		{"other task", &fakeAI{status: 200, body: `{"task_id":"zzz","status":"done"}`}, OutcomeTechnicalError, CodeRemoteTaskIDMismatch},
		{"too large", &fakeAI{status: 200, body: `{"task_id":"a1","status":"done","x":"` + strings.Repeat("a", 2000) + `"}`}, OutcomeTechnicalError, CodeAIResultTooLarge},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := newClient(t, tt.ai).Status(context.Background(), "REGISTER_COMPARE", "a1")
			if got.Outcome != tt.outcome || got.FailureCode != tt.code {
				t.Errorf("status = %+v, want %s %s", got, tt.outcome, tt.code)
			}
			if got.Outcome != OutcomeSucceeded && got.Result != nil {
				t.Error("only SUCCEEDED carries a result")
			}
		})
	}
}

func TestDonePassesAIsAnswerUnchanged(t *testing.T) {
	body := `{"task_id":"a1","status":"done","summary":{"counts":{"both":12}},"extraction_complete":true,"report_url":"http://x/r.docx","error":null}`
	got := newClient(t, &fakeAI{status: 200, body: body}).Status(context.Background(), "REGISTER_COMPARE", "a1")
	if got.Outcome != OutcomeSucceeded || string(got.Result) != body || got.FailureCode != "" {
		t.Errorf("status = %+v", got)
	}
}

func TestFailedMessages(t *testing.T) {
	got := newClient(t, &fakeAI{status: 200, body: `{"task_id":"a1","status":"error","error":"Нет доступа к файлу"}`}).
		Status(context.Background(), "REGISTER_COMPARE", "a1")
	if got.FailureMessage != "Нет доступа к файлу" {
		t.Errorf("message = %q", got.FailureMessage)
	}

	got = newClient(t, &fakeAI{status: 200, body: `{"task_id":"a1","status":"error","error":null}`}).
		Status(context.Background(), "REGISTER_COMPARE", "a1")
	if got.Outcome != OutcomeFailed || got.FailureMessage == "" {
		t.Errorf("FAILED needs a message even when AI gives none: %+v", got)
	}
}

func TestUnreachableAI(t *testing.T) {
	c, err := New(Config{BaseURLs: map[string]string{"REGISTER_COMPARE": "http://127.0.0.1:1", "SOCIAL_MEDIA": "http://127.0.0.1:1"}, Timeout: time.Second, MaxResponseBytes: 1000})
	if err != nil {
		t.Fatal(err)
	}
	if got := c.Status(context.Background(), "REGISTER_COMPARE", "a1"); got.Outcome != OutcomeTechnicalError || got.FailureCode != CodeAIUnavailable {
		t.Errorf("status = %+v", got)
	}
}

func TestUnknownOperation(t *testing.T) {
	ai := &fakeAI{status: 200}
	got := newClient(t, ai).Status(context.Background(), "ANALYZE_DECISIONS", "a1")
	if got.Outcome != OutcomeTechnicalError || got.FailureCode != CodeOperationUnsupported {
		t.Errorf("status = %+v", got)
	}
	if ai.path.Load() != nil {
		t.Error("AI must not be called")
	}
}

func TestNewNeedsEveryBaseURL(t *testing.T) {
	if _, err := New(Config{BaseURLs: map[string]string{"REGISTER_COMPARE": "http://x"}}); err == nil {
		t.Error("expected an error for the missing SOCIAL_MEDIA base URL")
	}
}
