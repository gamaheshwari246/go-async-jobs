# Assignment requirements

| Requirement | Implementation |
|---|---|
| Submit payload + priority + retries + timeout; return immediately | POST /jobs, spec.Validate, Asynq enqueue, HTTP 202 |
| Pluggable handlers | Asynq mux + demo.sleep |
| Concurrent independent workers | cmd/worker, WORKER_CONCURRENCY |
| Retry/exponential backoff | internal/backoff; persisted retry state in Redis |
| Dead letter on exhaustion | Asynq archived state |
| Status, attempts, error, result | GET /jobs/{id}; retention 24h |
| Queue depth | GET /queue/depth |
| Drain or cancellation | POST /admin/drain and POST /admin/resume; best-effort drain |
| Architecture diagram | docs/ARCHITECTURE.md |
| Delivery guarantees | docs/ARCHITECTURE.md |
| Unit retry tests | internal/backoff/backoff_test.go |
| Integration tests | internal/integration/integration_test.go, run with -tags=integration |
| GitHub Actions | .github/workflows/ci.yml |
| Setup + load limitations | README.md |

## Deliberate limitations
This demonstration is not an exactly-once payment processor. Redis AOF with everysec may lose recent writes on unexpected host crashes. Drain check/enqueue are not atomic. No strict per-submission idempotency and no real process-kill integration test. Results are retained for 24 hours. Quarantine/dead-letter records are subject to Asynq retention policy. Not all 0–9 priorities are strictly ordered; three weighted priority classes are supported.
