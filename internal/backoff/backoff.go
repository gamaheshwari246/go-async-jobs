package backoff
import "time"
const (BaseDelay=time.Second;MaxDelay=time.Minute)
func Delay(retryNumber int,jitter float64)time.Duration{
 if retryNumber<1{retryNumber=1}
 if jitter<0{jitter=0}
 if jitter>1{jitter=1}
 n:=retryNumber-1;if n>6{n=6}
 d:=BaseDelay*time.Duration(1<<n)
 if d>MaxDelay{d=MaxDelay}
 withJitter:=d+time.Duration(float64(d)*0.2*jitter)
 if withJitter>MaxDelay{return MaxDelay}
 return withJitter
}
