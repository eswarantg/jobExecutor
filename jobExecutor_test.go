package jobExecutor_test

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/eswarantg/jobExecutor"
)

func Test_NormalJob1(t *testing.T) {
	wg := sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())

	fmt.Printf("\nTest Started.")

	executor := jobExecutor.NewJobExecutor("NormalJob1", 10)
	executor.Debug = true
	wg.Add(1)
	go executor.Run(ctx, &wg)
	runtime.Gosched()

	job1 := TestJob{name: "NormalJob1", jobType: jobExecutor.QueueJob, when: time.Now(), maxduration: nil,
		durSleep: 5 * time.Second} //sleep dur 5 sec
	executor.AddJob(ctx, &job1)

	time.Sleep(2 * time.Second) //checking after 2 sec

	if job1.execTime.IsZero() {
		t.Errorf("\nExpected job to be started")
	}
	if !job1.cancelled.IsZero() {
		t.Errorf("\nExpected job to be not cancelled")
	}
	if !job1.exitTime.IsZero() {
		t.Errorf("\nExpected job to be not exited")
	}
	cancel()
	wg.Wait()
	fmt.Printf("\nTest Finished.")
}

func Test_NormalJob2(t *testing.T) {
	wg := sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())

	fmt.Printf("\nTest Started.")

	executor := jobExecutor.NewJobExecutor("NormalJob2", 10)
	executor.Debug = true
	wg.Add(1)
	go executor.Run(ctx, &wg)
	runtime.Gosched()

	job1 := TestJob{name: "NormalJob2", jobType: jobExecutor.QueueJob, when: time.Now(), maxduration: nil,
		durSleep: 1 * time.Second} //sleep dur 1 sec
	executor.AddJob(ctx, &job1)

	time.Sleep(2 * time.Second) //checking after 2 sec

	if job1.execTime.IsZero() {
		t.Errorf("\nExpected job to be started")
	}
	if !job1.cancelled.IsZero() {
		t.Errorf("\nExpected job to be not cancelled")
	}
	if job1.exitTime.IsZero() {
		t.Errorf("\nExpected job to be exited")
	}
	procTime := job1.exitTime.Sub(job1.execTime).Milliseconds()
	expTime := job1.durSleep.Milliseconds()
	margin := 1.0 / 100 //1%
	upperLimit := float64(expTime) * (1.0 + margin)
	lowerLimit := float64(expTime) * (1.0 - margin)
	if upperLimit >= float64(procTime) && float64(procTime) >= lowerLimit {
		fmt.Printf("\nExpTime %v matched %v (%v %% margin %v >= x >= %v)", expTime, procTime, margin*100, upperLimit, lowerLimit)
	} else {
		t.Errorf("\nExpTime %v DIDNT match %v (%v %% margin %v >= x >= %v)", expTime, procTime, margin*100, upperLimit, lowerLimit)
	}
	cancel()
	wg.Wait()
	fmt.Printf("\nTest Finished.")
}

func Test_NormalJob3(t *testing.T) {
	wg := sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())

	fmt.Printf("\nTest Started.")

	executor := jobExecutor.NewJobExecutor("NormalJob3", 10)
	executor.Debug = true
	wg.Add(1)
	go executor.Run(ctx, &wg)
	runtime.Gosched()

	deadline := time.Duration(2) * time.Second //2 second cancel deadling

	job1 := TestJob{name: "NormalJob3", jobType: jobExecutor.QueueJob, when: time.Now(), maxduration: &deadline,
		durSleep: -1 * time.Second} //sleep dur 2 - 1 = 1 sec
	executor.AddJob(ctx, &job1)

	time.Sleep(2 * time.Second) //checking after 2 sec

	if job1.execTime.IsZero() {
		t.Errorf("\nExpected job to be started")
	}
	if !job1.cancelled.IsZero() {
		t.Errorf("\nExpected job to be not cancelled")
	}
	if job1.exitTime.IsZero() {
		t.Errorf("\nExpected job to be exited")
	}
	procTime := job1.exitTime.Sub(job1.execTime).Milliseconds()
	expTime := job1.maxduration.Milliseconds() + job1.durSleep.Milliseconds()
	margin := 1.0 / 100 //1%
	upperLimit := float64(expTime) * (1.0 + margin)
	lowerLimit := float64(expTime) * (1.0 - margin)
	if upperLimit >= float64(procTime) && float64(procTime) >= lowerLimit {
		fmt.Printf("\nExpTime %v matched %v (%v %% margin %v >= x >= %v)", expTime, procTime, margin*100, upperLimit, lowerLimit)
	} else {
		t.Errorf("\nExpTime %v DIDNT match %v (%v %% margin %v >= x >= %v)", expTime, procTime, margin*100, upperLimit, lowerLimit)
	}
	cancel()
	wg.Wait()
	fmt.Printf("\nTest Finished.")
}

func Test_NormalJob4(t *testing.T) {
	wg := sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())

	fmt.Printf("\nTest Started.")

	executor := jobExecutor.NewJobExecutor("NormalJob4", 10)
	executor.Debug = true
	wg.Add(1)
	go executor.Run(ctx, &wg)
	runtime.Gosched()

	deadline := time.Duration(2) * time.Second //2 second cancel deadling

	job1 := TestJob{name: "NormalJob4", jobType: jobExecutor.QueueJob, when: time.Now(), maxduration: &deadline,
		durSleep: 1 * time.Second} //sleep dur 2 + 1 = 3 sec
	executor.AddJob(ctx, &job1)

	time.Sleep(4 * time.Second) //checking after 4 sec

	if job1.execTime.IsZero() {
		t.Errorf("\nExpected job to be started")
	}
	if job1.cancelled.IsZero() {
		t.Errorf("\nExpected job to be cancelled")
	}
	if !job1.exitTime.IsZero() {
		t.Errorf("\nExpected job not to be exited")
	}
	procTime := job1.cancelled.Sub(job1.execTime).Milliseconds()
	expTime := deadline.Milliseconds()
	margin := 1.0 / 100 //1%
	upperLimit := float64(expTime) * (1.0 + margin)
	lowerLimit := float64(expTime) * (1.0 - margin)
	if upperLimit >= float64(procTime) && float64(procTime) >= lowerLimit {
		fmt.Printf("\nExpTime %v matched %v (%v %% margin %v >= x >= %v)", expTime, procTime, margin*100, upperLimit, lowerLimit)
	} else {
		t.Errorf("\nExpTime %v DIDNT match %v (%v %% margin %v >= x >= %v)", expTime, procTime, margin*100, upperLimit, lowerLimit)
	}
	cancel()
	wg.Wait()
	fmt.Printf("\nTest Finished.")
}

func Test_NormalJob5(t *testing.T) {
	wg := sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())

	fmt.Printf("\nTest Started.")

	executor := jobExecutor.NewJobExecutor("NormalJob5", 10)
	executor.Debug = true
	wg.Add(1)
	go executor.Run(ctx, &wg)
	runtime.Gosched()

	deadline := time.Duration(2) * time.Second //2 second cancel deadling

	job1 := TestJob{name: "NormalJob5-1", jobType: jobExecutor.QueueJob, when: time.Now(), maxduration: &deadline,
		durSleep: -1 * time.Second} //sleep dur 2 - 1 = 2 sec
	executor.AddJob(ctx, &job1)

	deadline = time.Duration(1) * time.Second //1 second cancel deadling
	job2 := TestJob{name: "NormalJob5-2", jobType: jobExecutor.QueueJob, when: time.Now(), maxduration: &deadline,
		durSleep: 500 * time.Millisecond} //sleep dur 1 + 0.5 = 1.5 sec
	executor.AddJob(ctx, &job2)

	time.Sleep(2 * time.Second) //checking after 2 sec

	if job1.execTime.IsZero() {
		t.Errorf("\nExpected job to be started")
	}
	if !job1.cancelled.IsZero() {
		t.Errorf("\nExpected job to NOT be cancelled")
	}
	if job1.exitTime.IsZero() {
		t.Errorf("\nExpected job to be exited")
	}

	if job2.execTime.IsZero() {
		t.Errorf("\nExpected job to be started")
	}
	if job2.cancelled.IsZero() {
		t.Errorf("\nExpected job to be cancelled")
	}
	if !job2.exitTime.IsZero() {
		t.Errorf("\nExpected job NOT to be exited")
	}
	cancel()
	wg.Wait()
	fmt.Printf("\nTest Finished.")
}

func Test_OverriderJob1(t *testing.T) {
	wg := sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())

	fmt.Printf("\nTest Started.")

	executor := jobExecutor.NewJobExecutor("OverrideJob1", 10)
	executor.Debug = true
	wg.Add(1)
	go executor.Run(ctx, &wg)
	runtime.Gosched()

	deadline := time.Duration(2) * time.Second //2 second cancel deadling

	job1 := TestJob{name: "OverrideJob1", jobType: jobExecutor.CancelAndQueueJob, when: time.Now(), maxduration: &deadline,
		durSleep: -1 * time.Second} //sleep dur 2 - 1 = 1 sec
	executor.AddJob(ctx, &job1)

	time.Sleep(2 * time.Second) //checking after 2 sec

	if job1.execTime.IsZero() {
		t.Errorf("\nExpected job to be started")
	}
	if !job1.cancelled.IsZero() {
		t.Errorf("\nExpected job to NOT be cancelled")
	}
	if job1.exitTime.IsZero() {
		t.Errorf("\nExpected job to be exited")
	}
	cancel()
	wg.Wait()
	fmt.Printf("\nTest Finished.")
}

func Test_OverriderJob2(t *testing.T) {
	wg := sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())

	fmt.Printf("\nTest Started.")

	executor := jobExecutor.NewJobExecutor("OverrideJob2", 10)
	executor.Debug = true
	wg.Add(1)
	go executor.Run(ctx, &wg)
	runtime.Gosched()

	deadline := time.Duration(2) * time.Second //2 second cancel deadling

	job1 := TestJob{name: "NormalJob2", jobType: jobExecutor.QueueJob, when: time.Now(), maxduration: nil,
		durSleep: 5 * time.Second} //sleep dur 5 sec
	executor.AddJob(ctx, &job1)

	job1 = TestJob{name: "NormalJob3", jobType: jobExecutor.QueueJob, when: time.Now(), maxduration: nil,
		durSleep: 2 * time.Second} //sleep dur 5 sec
	executor.AddJob(ctx, &job1)

	time.Sleep(1 * time.Second)

	job2 := TestJob{name: "OverrideJob2", jobType: jobExecutor.CancelAndQueueJob, when: time.Now(), maxduration: &deadline,
		durSleep: -1 * time.Second} //sleep dur 2 - 1 = 1 sec
	executor.AddJob(ctx, &job2)

	time.Sleep(2 * time.Second) //checking after 2 sec

	if job1.execTime.IsZero() {
		t.Errorf("\nExpected job to be started")
	}
	if job1.cancelled.IsZero() {
		t.Errorf("\nExpected job to be cancelled")
	}
	if !job1.exitTime.IsZero() {
		t.Errorf("\nExpected job to be exited")
	}
	cancel()
	wg.Wait()
	fmt.Printf("\nTest Finished.")
}
