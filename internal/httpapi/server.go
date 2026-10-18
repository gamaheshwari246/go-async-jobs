package httpapi

import (
 "crypto/subtle"
 "crypto/sha256"
 "encoding/hex"
 "bytes"
 "encoding/json"
 "errors"
 "log/slog"
 "net/http"
 "strings"
 "time"

 "example.com/go-async-jobs/internal/operations"
 "example.com/go-async-jobs/internal/spec"
 "github.com/hibiken/asynq"
)

var queues=[]string{"critical","default","low"}
type Server struct{
 Client *asynq.Client
 Inspector *asynq.Inspector
 Logger *slog.Logger
 Gate operations.Gate
 AdminToken string
}
func(s *Server)Routes()http.Handler{
 mux:=http.NewServeMux()
 mux.HandleFunc("GET /healthz",func(w http.ResponseWriter,r *http.Request){writeJSON(w,http.StatusOK,map[string]string{"status":"up"})})
 mux.HandleFunc("POST /jobs",s.submit)
 mux.HandleFunc("GET /jobs/{id}",s.status)
 mux.HandleFunc("GET /queue/depth",s.depth)
 mux.HandleFunc("POST /admin/drain",s.adminDrain)
 mux.HandleFunc("POST /admin/resume",s.adminResume)
 mux.HandleFunc("GET /admin/drain",s.adminDrainStatus)
 return mux
}
func sha256Sum(key string)[]byte{sum:=sha256.Sum256([]byte(key));return sum[:]}
func(s *Server)submit(w http.ResponseWriter,r *http.Request){
 r.Body=http.MaxBytesReader(w,r.Body,64*1024)
 draining,err:=s.Gate.IsDraining(r.Context())
 if err!=nil{s.Logger.Error("admission gate error","error",err);http.Error(w,"queue temporarily unavailable",http.StatusServiceUnavailable);return}
 if draining{w.Header().Set("Retry-After","5");http.Error(w,"submissions disabled while draining",http.StatusServiceUnavailable);return}
 req,err:=spec.Decode(r.Body);if err!=nil{http.Error(w,err.Error(),http.StatusBadRequest);return}
 v,err:=spec.Validate(req);if err!=nil{http.Error(w,err.Error(),http.StatusBadRequest);return}
 task:=asynq.NewTask(v.Type,v.Body)
 opts:=[]asynq.Option{asynq.Queue(v.Queue),asynq.MaxRetry(v.Retries),asynq.Timeout(v.Timeout),asynq.Retention(24*time.Hour)}
 key:=r.Header.Get("Idempotency-Key")
 if key!=""{
   if len(key)>128{http.Error(w,"idempotency key too long",http.StatusBadRequest);return}
   for _,ch:=range key{if ch<33||ch>126{http.Error(w,"idempotency key must contain printable non-whitespace ASCII",http.StatusBadRequest);return}}
   sum:=sha256.Sum256([]byte(key))
   opts=append(opts,asynq.TaskID("idem-"+hex.EncodeToString(sum[:])))
 }
 info,err:=s.Client.EnqueueContext(r.Context(),task,opts...)
 if key!=""&&errors.Is(err,asynq.ErrTaskIDConflict){
   id:="idem-"+hex.EncodeToString(sha256Sum(key))
   for _,q:=range queues{
     existing,e:=s.Inspector.GetTaskInfo(q,id)
     if e==nil{
       if !bytes.Equal(existing.Payload,v.Body){http.Error(w,"idempotency key reused for different job configuration",http.StatusConflict);return}
       w.Header().Set("Location","/jobs/"+id)
       writeJSON(w,http.StatusOK,map[string]any{"job_id":id,"status":stateName(existing),"queue":q,"priority":v.Priority,"deduplicated":true});return
     }
   }
   http.Error(w,"duplicate task could not be inspected",http.StatusConflict);return
 }
 if err!=nil{s.Logger.Error("enqueue failed","error",err);http.Error(w,"queue temporarily unavailable",http.StatusServiceUnavailable);return}
 w.Header().Set("Location","/jobs/"+info.ID)
 writeJSON(w,http.StatusAccepted,map[string]any{"job_id":info.ID,"status":"queued","queue":info.Queue,"priority":v.Priority,"status_url":"/jobs/"+info.ID})
}
func stateName(info *asynq.TaskInfo)string{
 switch info.State{
 case asynq.TaskStateActive:return "running"
 case asynq.TaskStateCompleted:return "succeeded"
 case asynq.TaskStateRetry:return "failed"
 case asynq.TaskStateArchived:return "dead_lettered"
 default:return "queued"
 }
}
func attempts(info *asynq.TaskInfo)int{
 switch info.State{
 case asynq.TaskStatePending,asynq.TaskStateScheduled:return info.Retried
 case asynq.TaskStateRetry:return info.Retried
 default:return info.Retried+1
 }
}
func(s *Server)status(w http.ResponseWriter,r *http.Request){
 id:=r.PathValue("id")
 if len(id)<1||len(id)>128{http.Error(w,"invalid job id",http.StatusBadRequest);return}
 for _,q:=range queues{
 info,err:=s.Inspector.GetTaskInfo(q,id)
 if errors.Is(err,asynq.ErrQueueNotFound)||errors.Is(err,asynq.ErrTaskNotFound){continue}
 if err!=nil{s.Logger.Error("get task","error",err);http.Error(w,"queue temporarily unavailable",http.StatusServiceUnavailable);return}
 var job spec.Job
 _=json.Unmarshal(info.Payload,&job)
 response:=map[string]any{"job_id":info.ID,"type":info.Type,"queue":info.Queue,"priority":job.Priority,"status":stateName(info),"attempts":attempts(info),"max_retries":info.MaxRetry,"last_error":info.LastErr,"result":json.RawMessage("null")}
 if json.Valid(info.Result)&&len(info.Result)>0{response["result"]=json.RawMessage(info.Result)}
 if !info.NextProcessAt.IsZero(){response["next_run_at"]=info.NextProcessAt}
 if !info.CompletedAt.IsZero(){response["completed_at"]=info.CompletedAt}
 writeJSON(w,http.StatusOK,response);return
 }
 http.Error(w,"job not found (or retention expired)",http.StatusNotFound)
}
func(s *Server)depth(w http.ResponseWriter,r *http.Request){
 stats,err:=s.queueStats();if err!=nil{http.Error(w,"queue temporarily unavailable",http.StatusServiceUnavailable);return}
 writeJSON(w,http.StatusOK,stats)
}
func(s *Server)queueStats()(map[string]any,error){
 total,active,archived,completed:=0,0,0,0
 byQueue:=map[string]any{}
 existing,err:=s.Inspector.Queues()
 if err!=nil{return nil,err}
 present:=make(map[string]bool,len(existing))
 for _,name:=range existing{present[name]=true}
 for _,q:=range queues{
 if !present[q]{byQueue[q]=map[string]int{"waiting":0,"active":0,"archived":0,"completed":0};continue}
 info,err:=s.Inspector.GetQueueInfo(q)
 if errors.Is(err,asynq.ErrQueueNotFound){byQueue[q]=map[string]int{"waiting":0,"active":0,"archived":0,"completed":0};continue}
 if err!=nil{return nil,err}
 n:=info.Pending+info.Scheduled+info.Retry
 byQueue[q]=map[string]int{"waiting":n,"active":info.Active,"archived":info.Archived,"completed":info.Completed}
 total+=n;active+=info.Active;archived+=info.Archived;completed+=info.Completed
 }
 return map[string]any{"waiting":total,"active":active,"archived":archived,"completed_retained":completed,"by_queue":byQueue,"note":"waiting=pending+scheduled+retry; does not count active"},nil
}
func writeJSON(w http.ResponseWriter,status int,body any){
 w.Header().Set("Content-Type","application/json");w.WriteHeader(status);_=json.NewEncoder(w).Encode(body)
}
func(s *Server)adminAuthorized(w http.ResponseWriter,r *http.Request)bool{
 if s.AdminToken==""{http.Error(w,"admin API disabled (set ADMIN_TOKEN)",http.StatusServiceUnavailable);return false}
 given:=strings.TrimPrefix(r.Header.Get("Authorization"),"Bearer ")
 if len(given)!=len(s.AdminToken)||subtle.ConstantTimeCompare([]byte(given),[]byte(s.AdminToken))!=1{
 w.Header().Set("WWW-Authenticate","Bearer");http.Error(w,"unauthorized",http.StatusUnauthorized);return false
 }
 return true
}
func(s *Server)adminDrain(w http.ResponseWriter,r *http.Request){
 if !s.adminAuthorized(w,r){return}
 if err:=s.Gate.SetDraining(r.Context(),true);err!=nil{http.Error(w,"could not enable drain",http.StatusServiceUnavailable);return}
 writeJSON(w,http.StatusAccepted,map[string]any{"draining":true,"message":"new submissions rejected; queued and active work continues"})
}
func(s *Server)adminResume(w http.ResponseWriter,r *http.Request){
 if !s.adminAuthorized(w,r){return}
 if err:=s.Gate.SetDraining(r.Context(),false);err!=nil{http.Error(w,"could not resume",http.StatusServiceUnavailable);return}
 writeJSON(w,http.StatusOK,map[string]any{"draining":false})
}
func(s *Server)adminDrainStatus(w http.ResponseWriter,r *http.Request){
 if !s.adminAuthorized(w,r){return}
 draining,err:=s.Gate.IsDraining(r.Context());if err!=nil{http.Error(w,"redis unavailable",http.StatusServiceUnavailable);return}
 stats,err:=s.queueStats();if err!=nil{http.Error(w,"queue unavailable",http.StatusServiceUnavailable);return}
 waiting:=stats["waiting"].(int);active:=stats["active"].(int)
 writeJSON(w,http.StatusOK,map[string]any{"draining":draining,"drained":draining&&waiting==0&&active==0,"waiting":waiting,"active":active})
}
