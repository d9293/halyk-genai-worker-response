// Package ai asks the AI team's services how a task is doing. It only reports
// what AI said; Integrator.Api decides what follows (poll again, lost, done).
package ai

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Outcome values, as Integrator.Api expects them in an observation.
const (
	OutcomeQueued         = "QUEUED"
	OutcomeProcessing     = "PROCESSING"
	OutcomeSucceeded      = "SUCCEEDED"
	OutcomeFailed         = "FAILED"
	OutcomeNotFound       = "NOT_FOUND"
	OutcomeTechnicalError = "TECHNICAL_ERROR"
)

// Failure codes.
const (
	// AI finished the task with an error (status "error").
	CodeAITaskFailed = "AI_TASK_FAILED"
	// No answer in time.
	CodeAITimeout = "AI_TIMEOUT"
	// AI could not be reached or answered with an unexpected HTTP status
	// (5xx, a 404 that is not AI's "task not found", ...).
	CodeAIUnavailable = "AI_UNAVAILABLE"
	// AI answered 200 with a body the worker cannot read, or an unknown status.
	CodeAIBadResponse = "AI_BAD_RESPONSE"
	// AI answered about another task_id.
	CodeRemoteTaskIDMismatch = "REMOTE_TASK_ID_MISMATCH"
	// AI's answer is bigger than Integrator.Api accepts.
	CodeAIResultTooLarge = "AI_RESULT_TOO_LARGE"
	// The worker does not know where to ask for this operation.
	CodeOperationUnsupported = "WORKER_OPERATION_UNSUPPORTED"
)

// taskNotFoundDetail starts AI's agreed "task not found" answer (decision
// 2026-10-13): 404 {"detail": "Задача не найдена: <task_id>"}. Only this body
// means AI lost the task; any other 404 (a proxy, a wrong route) does not.
const taskNotFoundDetail = "Задача не найдена"

const maxMessageLen = 2000

// Status is what AI said about a task, ready to report to Integrator.Api.
type Status struct {
	Outcome string
	// Result is AI's answer, unchanged. Only for SUCCEEDED.
	Result         json.RawMessage
	FailureCode    string
	FailureMessage string
}

type Config struct {
	// BaseURLs per operation code, e.g. REGISTER_COMPARE → http://localhost:8081/register-compare.
	BaseURLs map[string]string
	// Timeout of one GET.
	Timeout time.Duration
	// MaxResponseBytes is the biggest answer passed on; Integrator.Api accepts
	// request bodies up to 1 MiB, so keep this a little below.
	MaxResponseBytes int64
}

type Client struct {
	// statusPaths per operation. Both services use the same path today; an
	// operation with a different status API gets its own entry.
	statusPaths map[string]string
	baseURLs    map[string]string
	maxBytes    int64
	http        *http.Client
}

func New(cfg Config) (*Client, error) {
	c := &Client{
		statusPaths: map[string]string{
			// Reestr (ud-registers-compare 1.0.0): GET /api/v1/tasks/{task_id}.
			"REGISTER_COMPARE": "/api/v1/tasks/",
			// Social media: not described by the AI team yet; follows the mock.
			"SOCIAL_MEDIA": "/api/v1/tasks/",
		},
		baseURLs: make(map[string]string),
		maxBytes: cfg.MaxResponseBytes,
		http:     &http.Client{Timeout: cfg.Timeout},
	}
	for op := range c.statusPaths {
		base := strings.TrimRight(cfg.BaseURLs[op], "/")
		if base == "" {
			return nil, fmt.Errorf("no AI base URL configured for %s", op)
		}
		c.baseURLs[op] = base
	}
	return c, nil
}

// Status asks AI about one task. It never fails: anything that is not a clear
// answer becomes TECHNICAL_ERROR, and Integrator.Api asks again later.
func (c *Client) Status(ctx context.Context, operation, taskID string) Status {
	path, ok := c.statusPaths[operation]
	if !ok {
		return technical(CodeOperationUnsupported, "The worker cannot check operation "+operation)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURLs[operation]+path+url.PathEscape(taskID), nil)
	if err != nil {
		return technical(CodeAIUnavailable, err.Error())
	}

	resp, err := c.http.Do(req)
	if err != nil {
		var timeout interface{ Timeout() bool }
		if errors.As(err, &timeout) && timeout.Timeout() {
			return technical(CodeAITimeout, "No answer from AI in time")
		}
		return technical(CodeAIUnavailable, "AI could not be reached: "+err.Error())
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, c.maxBytes+1))
	if err != nil {
		return technical(CodeAIUnavailable, "Reading AI's answer failed: "+err.Error())
	}
	if int64(len(data)) > c.maxBytes {
		return technical(CodeAIResultTooLarge, fmt.Sprintf("AI's answer is bigger than %d bytes", c.maxBytes))
	}
	return interpret(resp.StatusCode, data, taskID)
}

// interpret maps AI's answer to an outcome.
func interpret(status int, data []byte, taskID string) Status {
	switch status {
	case http.StatusOK:
	case http.StatusNotFound:
		if strings.HasPrefix(detail(data), taskNotFoundDetail) {
			return Status{Outcome: OutcomeNotFound}
		}
		return technical(CodeAIUnavailable, "AI answered HTTP 404 without its task-not-found answer: "+detail(data))
	default:
		return technical(CodeAIUnavailable, fmt.Sprintf("AI answered HTTP %d: %s", status, detail(data)))
	}

	var task struct {
		TaskID string  `json:"task_id"`
		Status string  `json:"status"`
		Error  *string `json:"error"`
	}
	if err := json.Unmarshal(data, &task); err != nil {
		return technical(CodeAIBadResponse, "AI answered 200 with an unreadable body: "+truncate(string(data)))
	}
	if task.TaskID != taskID {
		return technical(CodeRemoteTaskIDMismatch, fmt.Sprintf("AI answered about task_id %q, asked %q", task.TaskID, taskID))
	}
	switch task.Status {
	case "queued":
		return Status{Outcome: OutcomeQueued}
	case "processing":
		return Status{Outcome: OutcomeProcessing}
	case "done":
		return Status{Outcome: OutcomeSucceeded, Result: data}
	case "error":
		message := "AI reported an error without a message"
		if task.Error != nil && *task.Error != "" {
			message = *task.Error
		}
		return Status{Outcome: OutcomeFailed, FailureCode: CodeAITaskFailed, FailureMessage: truncate(message)}
	}
	return technical(CodeAIBadResponse, fmt.Sprintf("AI answered an unknown status %q", task.Status))
}

// detail reads FastAPI's {"detail": "..."}; otherwise the body itself.
func detail(data []byte) string {
	var body struct {
		Detail string `json:"detail"`
	}
	if json.Unmarshal(data, &body) == nil && body.Detail != "" {
		return truncate(body.Detail)
	}
	return truncate(strings.TrimSpace(string(data)))
}

func truncate(s string) string {
	if r := []rune(s); len(r) > maxMessageLen {
		return string(r[:maxMessageLen])
	}
	return s
}

func technical(code, message string) Status {
	return Status{Outcome: OutcomeTechnicalError, FailureCode: code, FailureMessage: truncate(message)}
}
