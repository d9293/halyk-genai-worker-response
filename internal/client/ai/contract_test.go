package ai

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

// TestContract runs the client against mock-ai-api (or a real AI service), to
// prove the two agree on paths and answers. Skipped unless CONTRACT_AI_URL is set:
//
//	CONTRACT_AI_URL=http://localhost:8081 go test -run Contract ./internal/client/ai/
//
// It sends one task to each operation, polls it to done, and checks the two
// kinds of 404.
func TestContract(t *testing.T) {
	baseURL := strings.TrimRight(os.Getenv("CONTRACT_AI_URL"), "/")
	if baseURL == "" {
		t.Skip("CONTRACT_AI_URL not set")
	}
	c, err := New(Config{
		BaseURLs:         map[string]string{"REGISTER_COMPARE": baseURL + "/register-compare", "SOCIAL_MEDIA": baseURL + "/social-media"},
		Timeout:          5 * time.Second,
		MaxResponseBytes: 1_000_000,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	tasks := map[string]string{
		"REGISTER_COMPARE": `{"task_id":%q,"files":["s3://bpm-bucket/a.pdf","s3://bpm-bucket/b.pdf"],"adjudicate":true}`,
		"SOCIAL_MEDIA":     `{"task_id":%q,"iin":"000000000000","full_name":"Контракт"}`,
	}
	paths := map[string]string{"REGISTER_COMPARE": "/register-compare/api/v1/compare", "SOCIAL_MEDIA": "/social-media/api/v1/search"}
	for op, body := range tasks {
		t.Run(op, func(t *testing.T) {
			taskID := newUUID()
			resp, err := http.Post(baseURL+paths[op], "application/json", bytes.NewBufferString(fmt.Sprintf(body, taskID)))
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusAccepted {
				t.Fatalf("submit: %d", resp.StatusCode)
			}

			var got Status
			for deadline := time.Now().Add(60 * time.Second); time.Now().Before(deadline); time.Sleep(time.Second) {
				got = c.Status(ctx, op, taskID)
				if got.Outcome != OutcomeQueued && got.Outcome != OutcomeProcessing {
					break
				}
			}
			if got.Outcome != OutcomeSucceeded || !strings.Contains(string(got.Result), taskID) {
				t.Fatalf("status = %+v, want SUCCEEDED with AI's answer", got)
			}

			if got := c.Status(ctx, op, newUUID()); got.Outcome != OutcomeNotFound {
				t.Errorf("unknown task: %+v, want NOT_FOUND", got)
			}
		})
	}

	wrongRoute, err := New(Config{
		BaseURLs:         map[string]string{"REGISTER_COMPARE": baseURL + "/no-such-service", "SOCIAL_MEDIA": baseURL},
		Timeout:          5 * time.Second,
		MaxResponseBytes: 1_000_000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := wrongRoute.Status(ctx, "REGISTER_COMPARE", newUUID()); got.Outcome != OutcomeTechnicalError {
		t.Errorf("wrong route: %+v, want TECHNICAL_ERROR (never NOT_FOUND)", got)
	}
}

func newUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}
