package jobExecutor_test

import (
	"context"
	"fmt"
	"time"

	"github.com/eswarantg/jobExecutor"
)

type TestJob struct {
	jobType     jobExecutor.Jobtype
	name        string
	when        time.Time
	maxduration *time.Duration

	//operation
	durSleep time.Duration

	//Observations
	execTime  time.Time
	cancelled time.Time
	exitTime  time.Time
}

func (t *TestJob) Type() jobExecutor.Jobtype {
	return t.jobType
}
func (t *TestJob) Name() string {
	return t.name
}
func (t *TestJob) When() time.Time {
	return t.when
}
func (t *TestJob) MaxDuration() *time.Duration {
	return t.maxduration
}
func (t *TestJob) Execute(ctx context.Context) error {
	defer func() {
		if t.cancelled.IsZero() {
			t.exitTime = time.Now()
			fmt.Printf("\nTest Job Execute Completed.")
		} else {
			fmt.Printf("\nTest Job Context Cancelled.")
		}
	}()
	fmt.Printf("\nTest Job Execute Starting.")
	t.execTime = time.Now()
	var sleepDur time.Duration
	if t.maxduration != nil {
		sleepDur = (*t.maxduration + t.durSleep)
	} else {
		sleepDur = t.durSleep
	}
	fmt.Printf("\nTest Job sleeping %v.", sleepDur)
	select {
	case <-ctx.Done():
		t.cancelled = time.Now()
	case <-time.After(sleepDur):
	}
	fmt.Printf("\nTest Job At return")
	return nil
}
