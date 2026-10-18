package integrator

import (
	"context"
	"encoding/json"
	"time"
)

// ClaimedResult is one finished task whose message this worker must publish to Kafka.
type ClaimedResult struct {
	RequestGUID string `json:"requestGuid"`
	// Topic and MessageKey (the Camunda requestId) of the Kafka message.
	Topic      string `json:"topic"`
	MessageKey string `json:"messageKey"`
	// Message is the Kafka message value. Kept as raw bytes so it is published
	// exactly as Integrator.Api built it, never re-encoded.
	Message    json.RawMessage `json:"message"`
	ClaimToken string          `json:"claimToken"`
	ClaimUntil time.Time       `json:"claimUntil"`
}

// ClaimResults takes up to limit results waiting to be published. Empty when none is.
func (c *Client) ClaimResults(ctx context.Context, limit int) ([]ClaimedResult, error) {
	var resp struct {
		Items []ClaimedResult `json:"items"`
	}
	err := c.post(ctx, "/internal/v1/results/claim", claimRequest{WorkerID: c.cfg.WorkerID, Limit: limit}, &resp)
	return resp.Items, err
}

// MarkPublished reports that Kafka accepted the message. Repeating it is safe.
func (c *Client) MarkPublished(ctx context.Context, r ClaimedResult) error {
	body := struct {
		ClaimToken string `json:"claimToken"`
	}{r.ClaimToken}
	return c.post(ctx, "/internal/v1/results/"+r.RequestGUID+"/published", body, nil)
}

// ReportPublishFailure reports that Kafka did not accept the message;
// Integrator.Api hands it out again later.
func (c *Client) ReportPublishFailure(ctx context.Context, r ClaimedResult, f Failure) error {
	body := struct {
		ClaimToken string  `json:"claimToken"`
		Failure    Failure `json:"failure"`
	}{r.ClaimToken, f}
	return c.post(ctx, "/internal/v1/results/"+r.RequestGUID+"/failure", body, nil)
}
