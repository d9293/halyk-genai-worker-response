# worker.Response

Part of the BPM ↔ AI integration. Takes work from
[Integrator.Api](https://github.com/d9293/halyk-genai-integrator) and does the
outside calls the API never makes itself:

- **polls:** ask AI how each task sent to it is doing, and report the answer
- **results:** publish each finished task's message to Kafka for BPM

It has no database and makes no decisions: Integrator.Api decides when to ask
again, when a task is lost and must be re-sent, and what the message says.
Several instances can run side by side; Integrator.Api's claims make sure no
two take the same work.

### Polls: report what AI said, nothing more

`GET {AI}/api/v1/tasks/{attemptId}`, mapped to an observation:

| AI answers | Observation |
|---|---|
| `200` `queued` / `processing` | `QUEUED` / `PROCESSING` |
| `200` `done` | `SUCCEEDED` + AI's whole answer, unchanged |
| `200` `error` | `FAILED`, `AI_TASK_FAILED` + AI's error text |
| `404 {"detail": "Задача не найдена: …"}` | `NOT_FOUND`: AI does not know the task |
| any other `404` (proxy, wrong route), `5xx`, timeout, odd reply | `TECHNICAL_ERROR`: asked again later |

Only AI's own "task not found" counts as `NOT_FOUND`. After 3 of them over at
least a minute Integrator.Api marks the attempt lost and worker.Request sends
the task again, so a wrong URL must never look like a lost task.

### Results: publish exactly what Integrator.Api built

The message (topic, key = Camunda `requestId`, value) comes ready from
Integrator.Api and is published byte for byte. Kafka must confirm with all
in-sync replicas; writes are idempotent. Then the result is marked published;
if Kafka fails, the failure is reported and Integrator.Api retries later.

If the worker dies between Kafka's confirmation and the report, the message
is published again. BPM ignores repeats by `requestGuid`.

## Run

Needs Integrator.Api on `INTEGRATOR_URL`, the AI services (mock-ai-api locally)
on `AI_*_URL` and Kafka on `KAFKA_BROKERS` (infra: `localhost:29092`). All
four apps together:
[local-run.md](https://github.com/d9293/halyk-genai-integrator/blob/main/docs/local-run.md).

```sh
make run      # go run . --config_file dev.env
```

Liveness: http://localhost:8084/health. It does not start if Kafka cannot be
reached.

On Ctrl+C / SIGTERM it stops claiming new work, lets work in progress finish
for up to `SHUTDOWN_GRACE`, then cancels what is left. Unfinished work is
handed out again by Integrator.Api once its claim expires.

## Docker

```sh
make docker   # builds halyk-genai-worker-response:local
docker run -p 8084:8084 -v $PWD/my.env:/config/app.env:ro halyk-genai-worker-response:local
```

The image holds only the binary (distroless, non-root, no shell); no config is
baked in, `*.env` files stay out of the build. Mount the env file at
`/config/app.env` (or pass another path with `--config_file`) and give secrets
as environment variables, which win over the file.

In Kubernetes, set `terminationGracePeriodSeconds` above `SHUTDOWN_GRACE` plus
10s for the last reports (45s for the default 30s); the default 30s would kill
work in progress.

## Develop

```sh
make test     # go test -race ./...
make mocks    # regenerate moq mocks

# Contract tests against the real counterparts (each skipped unless its variable is set):
CONTRACT_INTEGRATOR_URL=http://localhost:8080 go test -run Contract ./internal/client/integrator/  # API with SUBMISSION_POLL_DELAY=1s
CONTRACT_AI_URL=http://localhost:8081 go test -run Contract ./internal/client/ai/
CONTRACT_KAFKA_BROKERS=localhost:29092 go test -run Contract ./internal/broker/kafka/
```

## Layout

```
internal/app                wiring, start and orderly shutdown
internal/config             the only package that reads configuration
internal/runner             repeats a loop's rounds, with backoff when idle
internal/client/integrator  typed client for Integrator.Api's /internal/v1 worker API
internal/client/ai          AI status GET, mapped to an observation
internal/broker/kafka       sarama producer; the only package that knows sarama
internal/usecase/poll       the polls loop: claim → ask AI → observation
internal/usecase/result     the results loop: claim → publish → published / failure
```

`runner` and the core of `client/integrator` are copies of worker.Request's;
the two workers share no code on purpose, so each can change and deploy alone.
