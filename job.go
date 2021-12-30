package jobExecutor

import (
	"context"
	"time"
)

type Jobtype int

const (
	QueueJob Jobtype = iota
	CancelAndQueueJob
)

type Job interface {
	Type() Jobtype                 //Type of Job
	Name() string                  //Name of the Job for identifications
	When() time.Time               //When to run the job
	MaxDuration() *time.Duration   //Max Duration
	Execute(context.Context) error //Actual Execute function with the cancellable context passed
}
