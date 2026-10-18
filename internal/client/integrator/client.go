// Package integrator is worker.Response's client for Integrator.Api's internal
// worker API (/internal/v1). Types mirror the API's Swagger.
package integrator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

var (
	// ErrClaimLost: the claim expired or belongs to another worker. Drop the
	// work; never retry the call.
	ErrClaimLost = errors.New("claim lost")
	// ErrNotFound: the attempt or result does not exist.
	ErrNotFound = errors.New("not found")
	// ErrUnavailable: Integrator.Api could not be reached, also after retries.
	ErrUnavailable = errors.New("integrator unavailable")
)

// APIError is any other error answer from Integrator.Api, e.g. a 400 for a
// request the worker built wrongly. It is a bug, so it is not retried.
type APIError struct {
	Status  int
	Code    string
	Message string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("integrator answered %d %s: %s", e.Status, e.Code, e.Message)
}

type Config struct {
	// BaseURL of Integrator.Api, e.g. http://localhost:8080.
	BaseURL string
	// Timeout of one HTTP call.
	Timeout time.Duration
	// A call that cannot reach the API (network error, 502/503/504) is tried
	// again up to Retries times, RetryDelay apart.
	Retries    int
	RetryDelay time.Duration
	// WorkerID names this worker in claims.
	WorkerID string
}

type Client struct {
	cfg  Config
	http *http.Client
}

func New(cfg Config) *Client {
	cfg.BaseURL = strings.TrimRight(cfg.BaseURL, "/")
	return &Client{cfg: cfg, http: &http.Client{Timeout: cfg.Timeout}}
}

// post sends body as JSON and decodes a 2xx answer into out (if out is not nil).
// Every call to the worker API is safe to repeat (Integrator.Api recognizes
// repeats from its data), so calls that did not reach the API are retried.
func (c *Client) post(ctx context.Context, path string, body, out any) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("encode %s: %w", path, err)
	}

	var lastErr error
	for attempt := 0; attempt <= c.cfg.Retries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(c.cfg.RetryDelay):
			}
		}

		retry, err := c.once(ctx, path, payload, out)
		if !retry {
			return err
		}
		lastErr = err
	}
	return fmt.Errorf("%s: %w: %w", path, ErrUnavailable, lastErr)
}

// once makes one HTTP call. retry reports whether the API could not be reached.
func (c *Client) once(ctx context.Context, path string, payload []byte, out any) (retry bool, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.cfg.BaseURL+path, bytes.NewReader(payload))
	if err != nil {
		return false, fmt.Errorf("build request %s: %w", path, err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return false, ctx.Err() // shutting down: do not retry
		}
		return true, err // network error or timeout
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return true, fmt.Errorf("read answer of %s: %w", path, err)
	}

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		if out == nil {
			return false, nil
		}
		if err := json.Unmarshal(data, out); err != nil {
			return false, fmt.Errorf("decode answer of %s: %w", path, err)
		}
		return false, nil
	case resp.StatusCode == http.StatusBadGateway, resp.StatusCode == http.StatusServiceUnavailable,
		resp.StatusCode == http.StatusGatewayTimeout:
		return true, apiError(resp.StatusCode, data)
	}

	apiErr := apiError(resp.StatusCode, data)
	switch {
	case resp.StatusCode == http.StatusConflict && apiErr.Code == "CLAIM_LOST":
		return false, fmt.Errorf("%s: %w", path, ErrClaimLost)
	case resp.StatusCode == http.StatusNotFound:
		return false, fmt.Errorf("%s: %w: %w", path, ErrNotFound, apiErr)
	}
	return false, apiErr
}

// apiError reads Integrator.Api's error body: {"error": {"code", "message"}}.
func apiError(status int, data []byte) *APIError {
	var body struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	_ = json.Unmarshal(data, &body)
	return &APIError{Status: status, Code: body.Error.Code, Message: body.Error.Message}
}

// Failure describes an error to Integrator.Api.
type Failure struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type claimRequest struct {
	WorkerID string `json:"workerId"`
	Limit    int    `json:"limit"`
}
