package integrator

import (
	"context"
	"encoding/json"
	"time"
)

// ClaimedPoll is one task at AI whose status this worker must check now.
type ClaimedPoll struct {
	// AttemptID is the task_id AI knows the task by.
	AttemptID string `json:"attemptId"`
	// AttemptNo is 1 for the first send, 2 for a re-send after AI lost it, ...
	AttemptNo   int    `json:"attemptNo"`
	RequestGUID string `json:"requestGuid"`
	RequestID   string `json:"requestId"`
	// Operation tells which AI service to ask.
	Operation  string    `json:"operation"`
	ClaimToken string    `json:"claimToken"`
	ClaimUntil time.Time `json:"claimUntil"`
}

// Observation outcomes: what AI answered. Integrator.Api decides what follows.
const (
	// AI has the task and is not done.
	ObservationQueued     = "QUEUED"
	ObservationProcessing = "PROCESSING"
	// AI finished; Result holds AI's JSON.
	ObservationSucceeded = "SUCCEEDED"
	// AI finished with a business error; Failure describes it.
	ObservationFailed = "FAILED"
	// AI confirmed it does not know this task.
	ObservationNotFound = "NOT_FOUND"
	// No usable answer (timeout, 5xx, proxy 404); Failure describes it.
	ObservationTechnicalError = "TECHNICAL_ERROR"
)

// Observation is what the worker saw when it asked AI.
type Observation struct {
	Outcome string `json:"outcome"`
	// Result is AI's JSON, unchanged. Only for SUCCEEDED.
	Result json.RawMessage `json:"result,omitempty"`
	// Failure is required for FAILED and TECHNICAL_ERROR, not allowed otherwise.
	Failure *Failure `json:"failure,omitempty"`
}

// ClaimPolls takes up to limit tasks whose status check is due. Empty when none is.
func (c *Client) ClaimPolls(ctx context.Context, limit int) ([]ClaimedPoll, error) {
	var resp struct {
		Items []ClaimedPoll `json:"items"`
	}
	err := c.post(ctx, "/internal/v1/polls/claim", claimRequest{WorkerID: c.cfg.WorkerID, Limit: limit}, &resp)
	return resp.Items, err
}

// ReportObservation reports what AI answered. Repeating it is safe.
func (c *Client) ReportObservation(ctx context.Context, p ClaimedPoll, o Observation) error {
	body := struct {
		ClaimToken string `json:"claimToken"`
		Observation
	}{p.ClaimToken, o}
	return c.post(ctx, "/internal/v1/polls/"+p.AttemptID+"/observation", body, nil)
}
