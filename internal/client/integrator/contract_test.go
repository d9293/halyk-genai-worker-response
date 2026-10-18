package integrator

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

// TestContract runs the client against a real Integrator.Api, to prove the
// two agree on paths and JSON. Skipped unless CONTRACT_INTEGRATOR_URL is set:
//
//	CONTRACT_INTEGRATOR_URL=http://localhost:8080 go test -run Contract ./internal/client/integrator/
//
// It registers one SOCIAL_MEDIA task, sends it to "AI" in worker.Request's
// place, then polls it to SUCCEEDED and publishes its result. Start the API
// with a short SUBMISSION_POLL_DELAY (e.g. 1s) so the poll is due quickly.
func TestContract(t *testing.T) {
	baseURL := strings.TrimRight(os.Getenv("CONTRACT_INTEGRATOR_URL"), "/")
	if baseURL == "" {
		t.Skip("CONTRACT_INTEGRATOR_URL not set")
	}
	ctx := context.Background()
	c := New(Config{BaseURL: baseURL, Timeout: 5 * time.Second, Retries: 1, RetryDelay: 100 * time.Millisecond, WorkerID: "contract-test"})

	requestID, requestGUID := newUUID(), newUUID()
	call(t, baseURL+"/api/v1/tasks", fmt.Sprintf(`{"requestId":%q,"requestGuid":%q,"operation":"SOCIAL_MEDIA",
		"input":{"iin":"000000000000","fullName":"Контракт"},"documents":[]}`, requestID, requestGUID), http.StatusAccepted, nil)
	attemptID := submitAccepted(t, baseURL, requestGUID)

	// Poll: wait until the check is due, then report SUCCEEDED.
	var poll *ClaimedPoll
	for deadline := time.Now().Add(30 * time.Second); poll == nil && time.Now().Before(deadline); {
		polls, err := c.ClaimPolls(ctx, 10)
		if err != nil {
			t.Fatalf("claim polls: %v", err)
		}
		for i := range polls {
			if polls[i].RequestGUID == requestGUID {
				poll = &polls[i]
			} else {
				// Someone else's task: say it is still queued, which changes nothing.
				_ = c.ReportObservation(ctx, polls[i], Observation{Outcome: ObservationQueued})
			}
		}
		if poll == nil {
			time.Sleep(500 * time.Millisecond)
		}
	}
	if poll == nil {
		t.Fatal("the task was not handed out for polling within 30s (SUBMISSION_POLL_DELAY too long?)")
	}
	if poll.AttemptID != attemptID || poll.RequestID != requestID || poll.Operation != "SOCIAL_MEDIA" || poll.AttemptNo != 1 {
		t.Errorf("poll = %+v", poll)
	}
	aiResult := json.RawMessage(`{"task_id":"` + attemptID + `","status":"done","found":true,"profiles":[]}`)
	if err := c.ReportObservation(ctx, *poll, Observation{Outcome: ObservationSucceeded, Result: aiResult}); err != nil {
		t.Fatalf("report observation: %v", err)
	}

	// Result: claim it, check the message, mark it published.
	var result *ClaimedResult
	for range 10 {
		results, err := c.ClaimResults(ctx, 10)
		if err != nil {
			t.Fatalf("claim results: %v", err)
		}
		for i := range results {
			if results[i].RequestGUID == requestGUID {
				result = &results[i]
			}
		}
		if result != nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if result == nil {
		t.Fatal("the result was not handed out")
	}
	if result.MessageKey != requestID || result.Topic == "" {
		t.Errorf("result = %+v", result)
	}
	var msg struct {
		RequestGUID string          `json:"requestGuid"`
		Status      string          `json:"status"`
		Result      json.RawMessage `json:"result"`
	}
	if err := json.Unmarshal(result.Message, &msg); err != nil {
		t.Fatalf("message: %v", err)
	}
	if msg.RequestGUID != requestGUID || msg.Status != "SUCCEEDED" || !strings.Contains(string(msg.Result), `"found":true`) {
		t.Errorf("message = %s", result.Message)
	}
	if err := c.MarkPublished(ctx, *result); err != nil {
		t.Fatalf("mark published: %v", err)
	}
	if err := c.MarkPublished(ctx, *result); err != nil {
		t.Errorf("repeating published must be safe: %v", err)
	}
}

// submitAccepted does worker.Request's part: claims the task's submission and
// reports AI accepted it. It returns the attempt ID.
func submitAccepted(t *testing.T, baseURL, requestGUID string) string {
	t.Helper()
	for range 20 {
		var claim struct {
			Items []struct {
				AttemptID   string `json:"attemptId"`
				RequestGUID string `json:"requestGuid"`
				ClaimToken  string `json:"claimToken"`
			} `json:"items"`
		}
		call(t, baseURL+"/internal/v1/submissions/claim", `{"workerId":"contract-test","limit":1}`, http.StatusOK, &claim)
		for _, s := range claim.Items {
			outcome := `{"claimToken":%q,"outcome":"UNKNOWN"}` // someone else's task: polled, not lost
			if s.RequestGUID == requestGUID {
				outcome = `{"claimToken":%q,"outcome":"ACCEPTED","remoteStatus":"QUEUED"}`
			}
			call(t, baseURL+"/internal/v1/submissions/"+s.AttemptID+"/outcome", fmt.Sprintf(outcome, s.ClaimToken), http.StatusOK, nil)
			if s.RequestGUID == requestGUID {
				return s.AttemptID
			}
		}
	}
	t.Fatal("the task was not handed out for submission (AI places full?)")
	return ""
}

func call(t *testing.T, url, body string, wantStatus int, out any) {
	t.Helper()
	resp, err := http.Post(url, "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != wantStatus {
		t.Fatalf("POST %s: %d, want %d", url, resp.StatusCode, wantStatus)
	}
	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			t.Fatal(err)
		}
	}
}

func newUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}
