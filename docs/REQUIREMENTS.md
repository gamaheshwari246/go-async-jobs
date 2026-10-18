# Requirements-to-implementation checklist

| Assignment requirement | Actual implementation | Status / qualification |
|---|---|---|
| Submit payload, priority, max retries, timeout; return ID immediately | `POST /jobs`, `internal/spec`, Asynq enqueue, HTTP 202 | Implemented |
| Execution independent of HTTP | `cmd/worker`, Redis-backed Asynq | Implemented |
| Pluggable handler | Asynq ServeMux, `demo.sleep` and `demo.effect` | Implemented example handlers |
| Priority | Three weighted queues: low/default/critical | Not strict per-number ordering |
| Exponential backoff and per-attempt timeout | `internal/backoff`, Asynq timeout | Cooperative cancellation |
| Dead-letter past max retries | Asynq `archived` state | Redis-backed, not permanent storage |
| Status / attempts / error / result | `GET /jobs/{id}`, Asynq Inspector | Attempt count is based on retry metadata; result retention is limited |
| Queue-depth / drain | `GET /queue/depth`, `POST /admin/drain`, `/admin/resume` | Best-effort drain admission |
| Architecture flow diagram | [ARCHITECTURE.md](ARCHITECTURE.md) | Includes pending/active/retry/completed/archived paths |
| At-least-once semantics | [ARCHITECTURE.md](ARCHITECTURE.md) | Explicit failure windows, Redis durability limits |
| Duplicate detection/bounding | Server-generated task ID, retry count, Redis-local `demo.effect` and [IDEMPOTENCY.md](IDEMPOTENCY.md) | No exactly-once external business effect |
| Unit tests | `internal/backoff/backoff_test.go`, `internal/spec/job_test.go` | Included |
| Worker failure integration tests | `internal/integration/integration_test.go` | Handler failure, panic, timeout, retries, archived final state **and** real SIGKILL/crash-recovery with Redis effect deduplication |
| CI on push | `.github/workflows/ci.yml` | Runs Redis-backed suite with race detector, vet and build |
| Setup, worker config, under-load analysis | `README.md`, [ARCHITECTURE.md](ARCHITECTURE.md) | Included |

## Not mandatory or not guaranteed

**HTTP request idempotency** is optional; the assignment does **not** require callers to send `Idempotency-Key`. Worker retry detection always uses the server-created task ID. The header is implemented as an extension with a 48-hour queue-independent Redis reservation; enqueue and reservation remain separate operations, and the reservation can temporarily outlive a failed submission.

No permanent DLQ export, exhaustive failure-injection coverage, external-side-effect exactly-once guarantees, strict atomic admission/drain, hard backlog cap, or durability against every Redis/host failure. See the limitations and failure matrix in [ARCHITECTURE.md](ARCHITECTURE.md).

The GitHub Actions suite previously passed all unit and Redis integration tests as well as `go vet` and Go builds. Documentation-only changes should not alter runtime behavior.

## Hardening update

Payload types are registered centrally with dedicated validators and worker handlers in `internal/registry`. Redis queue-independent reservation and a cross-priority key test were added. Redis memory is bounded with `noeviction`, workers drain gracefully on ordinary stops, and the build pins dependency sums. A separate [future production architecture](FUTURE_PRODUCTION.md) sketches RabbitMQ + PostgreSQL without claiming it has been implemented.

## Submission reservation recovery

Optional HTTP idempotency now uses an atomic Redis fingerprint reservation with a five-second claim lease. A client retry recovers an orphaned pre-enqueue claim after lease expiry. If enqueue succeeded but confirmation was interrupted, Asynq inspection repairs the reservation. Integration tests cover both crash windows and cross-priority rejection. No new broker or database is required; this does not provide unconditional exactly-once semantics or automatic recovery without client retries.
