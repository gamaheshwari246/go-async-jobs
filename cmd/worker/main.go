package main

import (
	"log/slog"
	"math/rand"
	"os"
	"strconv"
	"time"

	"example.com/go-async-jobs/internal/backoff"
	"example.com/go-async-jobs/internal/observability"
	"example.com/go-async-jobs/internal/registry"

	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}

	concurrency := 5
	if raw := os.Getenv("WORKER_CONCURRENCY"); raw != "" {
		number, err := strconv.Atoi(raw)
		if err != nil || number < 1 || number > 100 {
			logger.Error("invalid_worker_concurrency", "value", raw, "allowed", "1..100")
			os.Exit(2)
		}
		concurrency = number
	}

	server := asynq.NewServer(asynq.RedisClientOpt{Addr: addr}, asynq.Config{
		Concurrency: concurrency,
		Queues: map[string]int{
			"critical": 6,
			"default":  3,
			"low":      1,
		},
		StrictPriority:  false,
		ShutdownTimeout: 330 * time.Second,
		RetryDelayFunc: func(retry int, err error, task *asynq.Task) time.Duration {
			delay := backoff.Delay(retry, rand.Float64())
			logger.Warn("job_retry_scheduled",
				"task_type", task.Type(),
				"retry_number", retry,
				"delay", delay.String(),
				"error", err,
			)
			return delay
		},
	})

	redisClient := redis.NewClient(&redis.Options{Addr: addr})
	defer redisClient.Close()

	mux := asynq.NewServeMux()
	registry.Register(mux, registry.Definitions(redisClient))
	mux.Use(observability.TaskMiddleware(logger))

	logger.Info("worker_starting", "redis_addr", addr, "concurrency", concurrency,
		"queue_weights", "critical:6 default:3 low:1")
	if err := server.Run(mux); err != nil {
		logger.Error("worker_stopped", "error", err)
		os.Exit(1)
	}
	logger.Info("worker_shutdown")
}
