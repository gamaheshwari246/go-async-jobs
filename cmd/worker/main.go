package main

import (
 "example.com/go-async-jobs/internal/backoff"
 "log/slog"
 "math/rand"
 "os"
 "strconv"
 "time"

 "example.com/go-async-jobs/internal/handlers"
 "github.com/hibiken/asynq"
)
func main(){
 logger:=slog.New(slog.NewJSONHandler(os.Stdout,nil))
 addr:=os.Getenv("REDIS_ADDR");if addr=="" {addr="127.0.0.1:6379"}
 concurrency:=5
 if s:=os.Getenv("WORKER_CONCURRENCY");s!=""{
 n,err:=strconv.Atoi(s)
 if err!=nil||n<1||n>100 {logger.Error("WORKER_CONCURRENCY must be 1..100");os.Exit(2)}
 concurrency=n
 }
 srv:=asynq.NewServer(asynq.RedisClientOpt{Addr:addr},asynq.Config{
 Concurrency:concurrency,
 RetryDelayFunc:func(n int,err error,task *asynq.Task)time.Duration{return backoff.Delay(n,rand.Float64())},
 Queues:map[string]int{"critical":6,"default":3,"low":1},StrictPriority:false,
 })
 mux:=asynq.NewServeMux();mux.HandleFunc("demo.sleep",handlers.DemoSleep)
 logger.Info("worker starting","concurrency",concurrency,"queues","critical:6 default:3 low:1")
 if err:=srv.Run(mux);err!=nil{logger.Error("worker stopped","error",err);os.Exit(1)}
}
