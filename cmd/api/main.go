package main

import (
 "context"
 "log/slog"
 "net/http"
 "os"
 "os/signal"
 "syscall"
 "time"

 "example.com/go-async-jobs/internal/httpapi"
 "example.com/go-async-jobs/internal/operations"
 "github.com/hibiken/asynq"
 "github.com/redis/go-redis/v9"
)
func getenv(name, fallback string) string {
 if v:=os.Getenv(name); v!="" {return v}; return fallback
}
func main() {
 logger:=slog.New(slog.NewJSONHandler(os.Stdout,nil))
 redisAddress:=getenv("REDIS_ADDR","127.0.0.1:6379")
 redisOpt:=asynq.RedisClientOpt{Addr:redisAddress}
 rdb:=redis.NewClient(&redis.Options{Addr:redisAddress})
 defer rdb.Close()
 client:=asynq.NewClient(redisOpt); defer client.Close()
 inspector:=asynq.NewInspector(redisOpt); defer inspector.Close()
 server:=&http.Server{
 Addr:getenv("HTTP_ADDR",":8080"),
 Handler:(&httpapi.Server{Client:client,Inspector:inspector,Logger:logger,Gate:operations.Gate{Redis:rdb},AdminToken:os.Getenv("ADMIN_TOKEN")}).Routes(),
 ReadHeaderTimeout:5*time.Second,ReadTimeout:10*time.Second,WriteTimeout:10*time.Second,IdleTimeout:60*time.Second,
 }
 ctx,stop:=signal.NotifyContext(context.Background(),syscall.SIGTERM,syscall.SIGINT);defer stop()
 go func(){<-ctx.Done();done,cancel:=context.WithTimeout(context.Background(),10*time.Second);defer cancel();_ = server.Shutdown(done)}()
 logger.Info("HTTP server starting","addr",server.Addr)
 if err:=server.ListenAndServe();err!=nil&&err!=http.ErrServerClosed {logger.Error("server exited","error",err);os.Exit(1)}
}
