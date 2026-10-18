# Two independent idempotency problems

## 1. Worker-execution duplicates — assignment requirement

A client calls `POST /jobs` without supplying any special header. The server generates Job 123 and enqueues it in Redis. **Every retry/redelivery still refers to Job 123.** This identity is managed by Asynq and requires no client-supplied key.

```text
Job 123 is enqueued
   -> Worker 1 runs the handler
   -> Redis-local effect is recorded
   -> Handler fails before success is recorded
   -> Asynq retries Job 123
   -> Worker 2 sees the existing Redis effect key
   -> No second Redis effect is recorded
   -> Handler returns success
```

The implementation is `internal/handlers/effect.go`: `SETNX("go-async-jobs:demo-effects:"+jobID, payload, 7-day TTL)`. It demonstrates an **atomic, Redis-local effect**, not exactly-once handler execution. The handler intentionally returns a simulated error after its first successful `SETNX` to reproduce the gap between effect and completion; the next attempt observes the key. Tests exercise this simulated failure **and** a separate real OS-worker SIGKILL/restart scenario (in `internal/integration/crash_recovery_test.go`) that verifies recovery and a single Redis-local effect. Neither test proves exactly-once arbitrary external effects.

`max_retries` bounds standard failed attempts, but does not guarantee no repeated execution, especially during process recovery or acknowledgment loss. Stable IDs and attempt logs **detect** repeats. They do not make external calls safe by themselves.

**Critical limitation:** `SETNX` and an unrelated external payment/email API call are not atomic. Do not mark payment complete in Redis before calling the provider and mistake that flag for exactly-once payment. Use a provider idempotency key or a business-database transaction that commits the unique operation ID with the business effect. The 7-day TTL and Redis persistence limitations also bound the demo's deduplication horizon.

## 2. HTTP duplicate submissions — optional, outside the assignment minimum

Two identical `POST /jobs` calls **without** a shared key might be two intentional jobs or a retry after a lost HTTP response. The server cannot infer intent. The optional `Idempotency-Key` header allows a client to identify one logical submission across HTTP retries:

- First request: enqueue with an Asynq task ID derived from SHA-256 of the header; return HTTP 202.
- Replayed matching request while the existing task is available: return the same task ID with HTTP 200.
- Same key but conflicting request configuration, when the existing task is detected: return HTTP 409.
- After retention expires, or when task identity is scoped differently across priority queues, this is **not** a permanent globally atomic deduplication guarantee.

The header is **not** used by Asynq when it retries a worker job; worker attempts already share the Asynq task ID. HTTP request deduplication is an additional convenience feature, not something explicitly requested in the assignment.

## Summary

| Concern | Key | Current capability | Caveat |
|---|---|---|---|
| Detect worker re-execution | Server-generated task ID | Asynq retry metadata + logging | Does not prevent re-execution |
| Demonstrate idempotent effect | Server-generated task ID | Redis `SETNX` for demo-local effect | Cannot protect unrelated APIs |
| Deduplicate client HTTP retries | Optional `Idempotency-Key` header | Deterministic task ID while retained | Limited scope/retention; not global guarantee |

See [ARCHITECTURE.md](ARCHITECTURE.md) for the full lifecycle, recovery and failure matrix.

## Queue-independent reservation implementation

A Redis Lua script atomically reserves the SHA-256 hash of the optional HTTP key with a fingerprint of the validated job body across **all** priority queues. A conflicting priority or payload receives HTTP 409 rather than enqueueing again. The reservation lasts 48 hours. This avoids concurrent cross-queue re-use but is not atomic with enqueue: a process crash after reservation and before successful enqueue can leave the key reserved without a job (replays may return HTTP 409 until reservation expiry). A production migration needs a durable request ledger or atomic outbox.
