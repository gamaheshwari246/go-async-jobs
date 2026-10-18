//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"example.com/go-async-jobs/internal/httpapi"
	"example.com/go-async-jobs/internal/operations"
 "example.com/go-async-jobs/internal/registry"
	"fmt"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

func TestFullLifecycle(t *testing.T) {
	addr := os.Getenv("REDIS_TEST_ADDR")
	if addr == "" {
		t.Skip("set REDIS_TEST_ADDR")
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr, DB: 14})
	defer rdb.Close()
	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	defer rdb.FlushDB(ctx)
	opt := asynq.RedisClientOpt{Addr: addr, DB: 14}
	client := asynq.NewClient(opt)
	defer client.Close()
	inspector := asynq.NewInspector(opt)
	defer inspector.Close()
	service := &httpapi.Server{Client: client, Inspector: inspector, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Gate: operations.Gate{Redis: rdb}, AdminToken: "integration-secret"}
	httpServer := httptest.NewServer(service.Routes())
	defer httpServer.Close()
	api := func(method, endpoint, request string, admin bool) (int, map[string]any) {
		t.Helper()
		req, err := http.NewRequest(method, httpServer.URL+endpoint, bytes.NewBufferString(request))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		if admin {
			req.Header.Set("Authorization", "Bearer integration-secret")
		}
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		var body map[string]any
		_ = json.NewDecoder(res.Body).Decode(&body)
		return res.StatusCode, body
	}
	submit := func(durationMS, failFirst, retry, timeout int) string {
		t.Helper()
		payload := fmt.Sprintf(`{"type":"demo.sleep","priority":9,"max_retries":%d,"timeout_seconds":%d,"payload":{"duration_ms":%d,"fail_first":%d}}`, retry, timeout, durationMS, failFirst)
		code, body := api("POST", "/jobs", payload, false)
		if code != http.StatusAccepted {
			t.Fatalf("POST status %d: %v", code, body)
		}
		return body["job_id"].(string)
	}
	poll := func(id, expected string, timeout time.Duration) map[string]any {
		t.Helper()
		deadline := time.Now().Add(timeout)
		for time.Now().Before(deadline) {
			code, body := api("GET", "/jobs/"+id, "", false)
			if code != http.StatusOK {
				t.Fatalf("GET %s: %d %v", id, code, body)
			}
			if body["status"] == expected {
				return body
			}
			time.Sleep(75 * time.Millisecond)
		}
		t.Fatalf("job %s did not reach %s", id, expected)
		return nil
	}
	id := submit(75, 0, 0, 5)
	code, depth := api("GET", "/queue/depth", "", false)
	if code != 200 || depth["waiting"].(float64) < 1 {
		t.Fatalf("queued depth: %d %v", code, depth)
	}
	_, first := api("GET", "/jobs/"+id, "", false)
	if first["status"] != "queued" {
		t.Fatalf("not queued: %v", first)
	}
	srv := asynq.NewServer(opt, asynq.Config{
		Concurrency: 3, Queues: map[string]int{"critical": 6, "default": 3, "low": 1},
		RetryDelayFunc:    func(int, error, *asynq.Task) time.Duration { return 125 * time.Millisecond },
		TaskCheckInterval: 100 * time.Millisecond, DelayedTaskCheckInterval: 100 * time.Millisecond,
	})
	mux := asynq.NewServeMux()
	registry.Register(mux,registry.Definitions(rdb))
	mux.HandleFunc("test.panic", func(context.Context, *asynq.Task) error { panic("simulated handler crash") })
	if err := srv.Start(mux); err != nil {
		t.Fatal(err)
	}
	defer srv.Shutdown()
	t.Run("success with result", func(t *testing.T) {
		info := poll(id, "succeeded", 12*time.Second)
		if info["attempts"] != float64(1) {
			t.Fatalf("attempts %v", info)
		}
		result, ok := info["result"].(map[string]any)
		if !ok || result["ok"] != true {
			t.Fatalf("missing result: %v", info)
		}
	})
	t.Run("transient retries", func(t *testing.T) {
		info := poll(submit(10, 2, 2, 5), "succeeded", 15*time.Second)
		if info["attempts"] != float64(3) {
			t.Fatalf("attempts %v", info)
		}
	})
	t.Run("dead letter", func(t *testing.T) {
		info := poll(submit(10, 5, 1, 5), "dead_lettered", 15*time.Second)
		if info["attempts"] != float64(2) || info["last_error"] == "" {
			t.Fatalf("DLQ %v", info)
		}
	})
	t.Run("panic archived", func(t *testing.T) {
		task := asynq.NewTask("test.panic", []byte(`{"priority":2,"payload":{}}`))
		info, err := client.Enqueue(task, asynq.Queue("low"), asynq.MaxRetry(0))
		if err != nil {
			t.Fatal(err)
		}
		state := poll(info.ID, "dead_lettered", 12*time.Second)
		if state["last_error"] == "" {
			t.Fatalf("panic not recorded %v", state)
		}
	})
	t.Run("timeout", func(t *testing.T) {
		info := poll(submit(2500, 0, 0, 1), "dead_lettered", 12*time.Second)
		if info["attempts"] != float64(1) {
			t.Fatalf("timeout attempts %v", info)
		}
	})
	t.Run("drain resume", func(t *testing.T) {
		code, _ := api("POST", "/admin/drain", "", false)
		if code != http.StatusUnauthorized {
			t.Fatalf("unprotected %d", code)
		}
		code, _ = api("POST", "/admin/drain", "", true)
		if code != http.StatusAccepted {
			t.Fatalf("drain %d", code)
		}
		code, _ = api("POST", "/jobs", `{"type":"demo.sleep","payload":{}}`, false)
		if code != http.StatusServiceUnavailable {
			t.Fatalf("admitted while draining %d", code)
		}
		code, status := api("GET", "/admin/drain", "", true)
		if code != 200 || status["draining"] != true {
			t.Fatalf("drain status %v", status)
		}
		code, _ = api("POST", "/admin/resume", "", true)
		if code != http.StatusOK {
			t.Fatalf("resume %d", code)
		}
		poll(submit(0, 0, 0, 5), "succeeded", 12*time.Second)
	})
	t.Run("duplicate submission", func(t *testing.T) {
		payload := `{"type":"demo.sleep","payload":{"duration_ms":0},"priority":5}`
		request := func(body string) (int, map[string]any) {
			req, err := http.NewRequest("POST", httpServer.URL+"/jobs", bytes.NewBufferString(body))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Idempotency-Key", "client-operation-001")
			res, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer res.Body.Close()
			var got map[string]any
			_ = json.NewDecoder(res.Body).Decode(&got)
			return res.StatusCode, got
		}
		a, x := request(payload)
		b, y := request(payload)
		if a != 202 || b != 200 || x["job_id"] != y["job_id"] {
			t.Fatalf("dedup %d %v / %d %v", a, x, b, y)
		}
		c, _ := request(`{"type":"demo.sleep","payload":{"duration_ms":30},"priority":5}`)
		if c != 409 {
			t.Fatalf("conflict %d", c)
		}
	})
	t.Run("effect committed once after retry", func(t *testing.T) {
		code, res := api("POST", "/jobs", `{"type":"demo.effect","payload":{},"max_retries":1}`, false)
		if code != 202 {
			t.Fatalf("submit effect %d %v", code, res)
		}
		id := res["job_id"].(string)
		outcome := poll(id, "succeeded", 15*time.Second)
		if outcome["attempts"] != float64(2) {
			t.Fatalf("effect attempts %v", outcome)
		}
		key := "go-async-jobs:demo-effects:" + id
		n, err := rdb.Exists(ctx, key).Result()
		if err != nil || n != 1 {
			t.Fatalf("effect missing %v %v", n, err)
		}
	})

 t.Run("cross-queue idempotency",func(t *testing.T){
  key:="priority-cross-queue-001"
  submitKey:=func(priority int)(int,map[string]any){
   raw:=fmt.Sprintf(`{"type":"demo.sleep","priority":%d,"payload":{}}`,priority)
   req,err:=http.NewRequest("POST",httpServer.URL+"/jobs",bytes.NewBufferString(raw));if err!=nil{t.Fatal(err)}
   req.Header.Set("Idempotency-Key",key)
   res,err:=http.DefaultClient.Do(req);if err!=nil{t.Fatal(err)}
   defer res.Body.Close()
   var got map[string]any
   _=json.NewDecoder(res.Body).Decode(&got)
   return res.StatusCode,got
  }
  first,one:=submitKey(2)
  second,_:=submitKey(9)
  if first!=202||second!=409{t.Fatalf("same idempotency key different queues: %d %v, second %d",first,one,second)}
 })
	t.Run("invalid priority", func(t *testing.T) {
		code, _ := api("POST", "/jobs", `{"type":"demo.sleep","priority":100,"payload":{}}`, false)
		if code != http.StatusBadRequest {
			t.Fatalf("priority accepted %d", code)
		}
	})
}
