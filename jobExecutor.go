package jobExecutor

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"sync"
	"time"
)

//JobExecutor  - Execute jobs queued in seperate go routines
type JobExecutor struct {
	name             string                        //name of the Job
	channel          chan Job                      //Channel to get inputs
	overrideChannel  chan Job                      //Channel for override existing job
	cancelFuncs      *map[int64]context.CancelFunc //Cancel functions of the jobs running
	Debug            bool                          //Debug - set to true will print additional logs on stdout
	queingInProgress int64                         //queingInProgress - when queuing has to be done in both queues
	minSleep         time.Duration                 //lowest resolution time below which sleep is ineffective
	addMutex         sync.Mutex                    //allow adding
}

//NewJobExecutor - Create a new JobExecutor
func NewJobExecutor(name string, queueSize int) *JobExecutor {
	return &JobExecutor{
		name:             name,
		channel:          make(chan Job, queueSize),
		overrideChannel:  make(chan Job, queueSize),
		cancelFuncs:      nil,
		queingInProgress: 0,
		minSleep:         time.Duration(100) * time.Millisecond,
		addMutex:         sync.Mutex{},
	}
}

func (s *JobExecutor) GetMinSleep() time.Duration {
	return s.minSleep
}

func (s *JobExecutor) SetMinSleep(d time.Duration) {
	s.minSleep = d
}

//Current Queue of jobs waiting
func (s *JobExecutor) WaitingQueueLen() int {
	return len(s.channel)
}

//Current jobs executing
func (s *JobExecutor) ExecutingQueueLen() int {
	if s.cancelFuncs != nil {
		return len(*s.cancelFuncs)
	}
	return 0
}

//AddJob - add a job for execution
func (s *JobExecutor) AddJob(ctx context.Context, j Job) {
	if j == nil {
		return
	}
	l := len(s.channel)
	now := time.Now()
	if s.Debug {
		fmt.Fprintf(os.Stdout, "\n%v %v JobExecutor queing %v[QueueLen:%v].", now.UTC(), s.name, j.Name(), l+1)
	}
	if j.Type() == CancelAndQueueJob {
		//prevent dequeue till both jobs are added
		s.addMutex.Lock() //need to queue both of them
		defer s.addMutex.Unlock()
		s.overrideChannel <- j
		s.channel <- j
	} else {
		s.channel <- j
	}
}

func (s *JobExecutor) executeJob(ctx context.Context, j Job, jobId int64, jobFinished chan<- int64, childWg *sync.WaitGroup) {
	defer func() {
		if childWg != nil {
			childWg.Done()
		}
		if s.Debug {
			fmt.Fprintf(os.Stdout, "\n%v %v Job %v(%v) finished.", time.Now().UTC(), s.name, j.Name(), jobId)
		}
		if ctx.Err() == nil {
			jobFinished <- jobId
		}
	}()
	sleepDur := time.Until(j.When())
	if sleepDur > s.minSleep {
		if s.Debug {
			fmt.Fprintf(os.Stdout, "\n%v %v Job %v(%v) waiting %v.", time.Now().UTC(), s.name, j.Name(), jobId, sleepDur)
		}
		select {
		case <-ctx.Done():
			if s.Debug {
				fmt.Fprintf(os.Stdout, "\n%v %v Job %v(%v) cancelled while waiting.", time.Now().UTC(), s.name, j.Name(), jobId)
			}
			return
		case <-time.After(sleepDur):
		}
	}
	if s.Debug {
		fmt.Fprintf(os.Stdout, "\n%v %v Job %v(%v) started.", time.Now().UTC(), s.name, j.Name(), jobId)
	}
	err := j.Execute(ctx) //Execute the job
	if err != nil && ctx.Err() == nil {
		fmt.Fprintf(os.Stderr, "\n%v %v %v(%v) error: %v", time.Now().UTC(), s.name, j.Name(), jobId, err.Error())
	}
}

func (s *JobExecutor) cancelJobs(ctx context.Context, jobName string) (towait bool) {
	if s.cancelFuncs == nil {
		return
	}
	for k, cf := range *s.cancelFuncs {
		if s.Debug {
			fmt.Fprintf(os.Stdout, "\n%v %v JobExecutor to %v, cancelling %v", time.Now().UTC(), s.name, jobName, k)
		}
		cf()
		towait = true
		delete(*s.cancelFuncs, k)
	}
	return
}

func (s *JobExecutor) checkForNewOverride(ctx context.Context, overrideJob Job, normalJob Job) (Job, Job) {
	if overrideJob == nil {
		if normalJob == nil {
			panic("both cannot be nil")
		}
	} else {
		if normalJob != nil {
			panic("both cannot be NOT nil")
		}
	}
	ret := overrideJob //nil return if there is no override jobs
	for len(s.overrideChannel) > 0 {
		var j Job
		var channelOpen bool
		select {
		case <-ctx.Done(): //ctx cancelled
		case j, channelOpen = <-s.overrideChannel: //override requested
			if !channelOpen {
				break
			}
			if normalJob != nil {
				if j == normalJob {
					//found the matching override job picked up
					ret = normalJob
					normalJob = nil
				} //else {
				//existing normalJob should also be cancelled
				//will happen in the next loop
				//}
			}
			if j != nil && ret != j {
				if s.Debug {
					fmt.Fprintf(os.Stdout, "\n%v %v JobExecutor rejecting on override %v for %v.", time.Now().UTC(), s.name, ret.Name(), j.Name())
				}
				ret = j
			}
		}
	}
	return ret, normalJob
}

func (s *JobExecutor) clearPendingNormalJobs(ctx context.Context, overrideJob Job, normalJob Job) Job {
	var channelOpen bool
	for len(s.channel) > 0 && overrideJob != nil && normalJob != overrideJob {
		select {
		case <-ctx.Done(): //ctx cancelled
		case normalJob, channelOpen = <-s.channel: //override requested
			if !channelOpen {
				break
			}
			if normalJob != nil && overrideJob != normalJob {
				if s.Debug {
					fmt.Fprintf(os.Stdout, "\n%v %v JobExecutor rejecting on override %v for %v.", time.Now().UTC(), s.name, normalJob.Name(), overrideJob.Name())
				}
			}
		}
	}
	return normalJob
}

//Run - Deamon that dequeues the jobs and executes them
func (s *JobExecutor) Run(ctx context.Context, wg *sync.WaitGroup) {
	//No MUTEX gaurd etc done... as expect disipline to invoke only once
	//This check is just for accidental second Run
	if s.cancelFuncs != nil {
		if s.Debug {
			fmt.Fprintf(os.Stdout, "\n%v %v JobExecutor Already running, Exiting.", time.Now().UTC(), s.name)
		}
		return
	}
	defer func() {
		//Signal closure for the outer context
		if wg != nil {
			defer wg.Done()
		}
		if s.Debug {
			fmt.Fprintf(os.Stdout, "\n%v %v JobExecutor Finished.", time.Now().UTC(), s.name)
		}
		s.cancelFuncs = nil
	}()

	if s.Debug {
		fmt.Fprintf(os.Stdout, "\n%v %v JobExecutor Started.", time.Now().UTC(), s.name)
	}

	//init
	cancelFuncs := make(map[int64]context.CancelFunc)
	s.cancelFuncs = &cancelFuncs
	nextJobId := int64(0)

	//create channel for child to report completion
	jobFinished := make(chan int64, 10)
	defer close(jobFinished)

	var overrideJob Job
	var normalJob Job
	var channelOpen bool

	var childWg sync.WaitGroup //WaitGroup for synching the child Job
	defer childWg.Wait()       //Wait for all children to complete
	defer func() {
		if ctx.Err() == nil {
			s.cancelJobs(ctx, "ExitRun") //cancel all executing jobs
		}
	}()

	waitingForCancelComplete := false //right now cancelling all running jobs
	checkingForNewOverride := false   //right now ignoring current selected override with any future overide that might have come

OuterLoop:
	for {
		//WaitingCancelCompleteLoop:
		if waitingForCancelComplete {
			toWait := s.cancelJobs(ctx, overrideJob.Name())
			if toWait {
				childWg.Wait()
			}
		}
		waitingForCancelComplete = false
		//CheckingForNewOverrideLoop:
		if checkingForNewOverride {
			overrideJob, normalJob = s.checkForNewOverride(ctx, overrideJob, normalJob)
			if ctx.Err() != nil {
				break
			}
		}
		if overrideJob != nil {
			normalJob = s.clearPendingNormalJobs(ctx, overrideJob, normalJob)
			overrideJob = nil
			if normalJob == nil {
				panic("normal job cannot be nil")
			}
		}
		if normalJob != nil {
			var childCtx context.Context
			var childCancelFunc context.CancelFunc //cancel function for the child Job cancel trigger
			if s.Debug {
				fmt.Fprintf(os.Stdout, "\n%v %v JobExecutor job %v setting up to execute.", time.Now().UTC(), s.name, normalJob.Name())
			}
			nextJobId++ //create jobId
			maxDur := normalJob.MaxDuration()
			if maxDur != nil && (*maxDur) > 0 { //If there is MaxDuration given
				childCtx, childCancelFunc = context.WithTimeout(ctx, *maxDur) //create a Timeout context
			} else {
				childCtx, childCancelFunc = context.WithCancel(ctx) //create a cancel context
			}
			cancelFuncs[nextJobId] = childCancelFunc
			childWg.Add(1)
			go s.executeJob(childCtx, normalJob, nextJobId, jobFinished, &childWg)
			runtime.Gosched()
			normalJob = nil
		}
	WaitingForJobs:
		for {
			select {
			case <-ctx.Done(): //ctx cancelled
				break OuterLoop
			case id, channelOpen := <-jobFinished: //Job finished
				if !channelOpen {
					break OuterLoop
				}
				if s.Debug {
					fmt.Fprintf(os.Stdout, "\n%v %v JobExecutor job %v finished.", time.Now().UTC(), s.name, id)
				}
				cf, ok := cancelFuncs[id]
				if ok {
					cf()
					delete(cancelFuncs, id)
				}
			case overrideJob, channelOpen = <-s.overrideChannel: //override requested
				if !channelOpen {
					break OuterLoop
				}
				if s.Debug {
					fmt.Fprintf(os.Stdout, "\n%v %v JobExecutor picked new overrideJob : %v", time.Now().UTC(), s.name, overrideJob.Name())
				}
				s.addMutex.Lock() //Wait for both jobs to be added in both queues ... normalJob queue will definitely have entry
				s.addMutex.Unlock()
				if overrideJob == nil {
					continue
				}
				waitingForCancelComplete = true //right now cancelling all running jobs
				checkingForNewOverride = true   //right now ignoring current selected override with any future overide that might have come
				break WaitingForJobs
			case normalJob, channelOpen = <-s.channel: //new normal job
				if !channelOpen {
					break OuterLoop
				}
				//Job picked up may be a normalJob or a overrideJob... just read from normal queue first
				if s.Debug {
					fmt.Fprintf(os.Stdout, "\n%v %v JobExecutor picked new normalJob : %v", time.Now().UTC(), s.name, normalJob.Name())
				}
				if normalJob == nil {
					continue
				}
				waitingForCancelComplete = false //right now cancelling all running jobs
				checkingForNewOverride = true    //right now ignoring current selected override with any future overide that might have come
				break WaitingForJobs
			}
		}
	}
}
