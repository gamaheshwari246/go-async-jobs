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
| Worker failure integration tests | `internal/integration/integration_test.go` | Handler failure, panic, timeouts, retries, archived final state; not a process-kill test |
| CI on push | `.github/workflows/ci.yml` | Runs Redis-backed suite with race detector, vet and build |
| Setup, worker config, under-load analysis | `README.md`, [ARCHITECTURE.md](ARCHITECTURE.md) | Included |

## Not mandatory or not guaranteed

**HTTP request idempotency** is optional; the assignment does **not** require callers to send `Idempotency-Key`. Worker retry detection always uses the server-created task ID. The header is implemented as an extension but has retention and cross-queue scope limitations.

No permanent DLQ export, process kill/restart test, external-side-effect exactly-once guarantees, strict atomic admission/drain, hard backlog cap, or durability against every Redis/host failure. See the limitations and failure matrix in [ARCHITECTURE.md](ARCHITECTURE.md).

The GitHub Actions suite previously passed all unit and Redis integration tests as well as `go vet` and Go builds. Documentation-only changes should not alter runtime behavior.
