// Package idempotency manages a queue-independent Redis reservation for HTTP
// Idempotency-Key. It is not an exactly-once transaction with Asynq enqueue.
package idempotency

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/redis/go-redis/v9"
)

type Decision int

const (
	New Decision = iota
	Existing
	Conflict
)
const lifetime = 48 * time.Hour

var reserveScript = redis.NewScript(`
local old=redis.call('GET',KEYS[1])
if old then
 if old==ARGV[1] then return 1 end
 return 2
end
redis.call('SET',KEYS[1],ARGV[1],'PX',ARGV[2])
return 0
`)

// Reserve atomically prevents the same key from being accepted in two queues.
// A process crash between reservation and enqueue may leave a temporary orphan
// until the TTL expires. This is deliberately documented, not hidden.
func Reserve(ctx context.Context, client redis.UniversalClient, key string, body []byte) (Decision, error) {
	keyHash := sha256.Sum256([]byte(key))
	hash := sha256.Sum256(body)
	slot := "go-async-jobs:submission-idem:" + hex.EncodeToString(keyHash[:])
	n, err := reserveScript.Run(ctx, client, []string{slot}, hex.EncodeToString(hash[:]), lifetime.Milliseconds()).Int()
	return Decision(n), err
}
