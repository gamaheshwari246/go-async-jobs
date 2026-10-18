package handlers
import(
 "context"
 "encoding/json"
 "errors"
 "fmt"
 "time"
 "example.com/go-async-jobs/internal/spec"
 "github.com/hibiken/asynq"
 "github.com/redis/go-redis/v9"
)
func EffectHandler(client redis.UniversalClient)asynq.HandlerFunc{
 return func(ctx context.Context,task *asynq.Task)error{
 var job spec.Job
 if err:=json.Unmarshal(task.Payload(),&job);err!=nil{return err}
 id,ok:=asynq.GetTaskID(ctx);if !ok||id==""{return errors.New("missing job ID")}
 key:="go-async-jobs:demo-effects:"+id
 first,err:=client.SetNX(ctx,key,string(job.Payload),7*24*time.Hour).Result()
 if err!=nil{return fmt.Errorf("record effect: %w",err)}
 retried,_:=asynq.GetRetryCount(ctx)
 if first&&retried==0{return errors.New("simulated crash after committing effect")}
 result,_:=json.Marshal(map[string]any{"ok":true,"job_id":id,"effect_performed_on_this_attempt":first})
 _,err=task.ResultWriter().Write(result);return err
 }
}
