package work

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/OneOfOne/xxhash"
	"go.uber.org/zap"
)

// TeamConfig contains configuration values for a work Team.
type TeamConfig struct {
	// Optional Name for the Team.
	Name string
	// Number of workers for the Team.
	Workers int
	// WorkerQueueSize is the queue size allowed for each worker.
	WorkerQueueSize int
	// MaxTime in seconds allowed for any Task submits or Result requests.
	MaxTimeSecs int
	// CloseOnTimeout closes any open Result channels open on Requests if the operation times out.
	CloseOnTimeout bool
}

// NewTeamConfig returns a new TeamConfig with defaults.
func NewTeamConfig() *TeamConfig {
	return &TeamConfig{
		Name:            "Default",
		Workers:         10,
		WorkerQueueSize: 20,
		MaxTimeSecs:     1,
		CloseOnTimeout:  true,
	}
}

// Team is the coordinating members performing work.
type Team struct {
	Name           string
	Config         *TeamConfig
	Logger         *zap.Logger
	RequestChannel chan TaskRequest
	workers        []chan TaskRequest
	requestMap     RequestMap
	stopChan       chan struct{}
	sync           syncGroup
}

type syncGroup struct {
	mainSync   *sync.WaitGroup
	workerSync *sync.WaitGroup
}

// NewTeam creates a new team with the given config options.
func NewTeam(config *TeamConfig) *Team {
	if config == nil {
		config = NewTeamConfig()
	}
	return &Team{
		Name:           config.Name,
		Config:         config,
		Logger:         newNopLogger(),
		RequestChannel: make(chan TaskRequest),
		workers:        make([]chan TaskRequest, config.Workers),
		requestMap:     NewRequestMap(),
		sync: syncGroup{
			mainSync:   &sync.WaitGroup{},
			workerSync: &sync.WaitGroup{},
		},
		stopChan: make(chan struct{}),
	}
}

// SetName sets the Team Name on both the Config and Team.
func (t *Team) SetName(name string) {
	t.Config.Name = name
	t.Name = name
}

// AddTask adds a defined RequestHandleFunc for a RequestTypeID.
func (t *Team) AddTask(id int, requestFunc RequestHandleFunc) {
	t.requestMap.All[id] = requestFunc
}

// AddConsist is the same as AddTask with the exception that the RequestTypeID should be serviced by a consistent worker.
func (t *Team) AddConsist(id int, requestFunc RequestHandleFunc) {
	t.requestMap.All[id] = requestFunc
	t.requestMap.Consistent[id] = requestFunc
}

// Submit sends a Request to the Team with a timeout specified in seconds.
// If the request is sent, return true. Otherwise, if the timeout is hit, return false.
// If CloseOnTimeout is true, the Result channel will be closed if open.
func (t *Team) Submit(request TaskRequest) bool {
	t.sync.mainSync.Add(1)
	timeout := time.After(time.Duration(t.Config.MaxTimeSecs) * time.Second)
	select {
	case t.RequestChannel <- request:
		t.Logger.Debug("Submit Request Successful", zap.String("Team", t.Name), zap.String("RequestType", fmt.Sprintf("%T:%+v", request.ReqType(), request.ReqType())))
		t.sync.mainSync.Done()
		return true
	case <-timeout:
		t.Logger.Debug("Submit Request TIMED OUT", zap.String("Team", t.Name), zap.String("RequestType", fmt.Sprintf("%T:%+v", request.ReqType(), request.ReqType())))
		if t.Config.CloseOnTimeout {
			if request.ResultChan() != nil {
				close(request.ResultChan())
			}
		}
		t.sync.mainSync.Done()
		return false
	}
}

// Start starts all workers and Listner.
func (t *Team) Start() {
	ensureLogger(t)
	for i := 0; i < len(t.workers); i++ {
		t.workers[i] = make(chan TaskRequest, t.Config.WorkerQueueSize)
		t.sync.mainSync.Add(1)
		t.sync.workerSync.Add(1)
		t.Logger.Debug("Starting Worker", zap.String("Team", t.Name), zap.Int("WorkerID", i))
		go t.startWorker(i, t.workers[i]) //, t.requestMap.All, &t.sync)
	}
	// wait for all workers to start
	t.sync.mainSync.Wait()
	// add sync for listener
	t.sync.mainSync.Add(1)
	go t.listen()
}

// Stop here.
func (t *Team) Stop() {
	t.Logger.Debug("Stop Received.", zap.String("Team", t.Name))
	t.Logger.Debug("Stopping", zap.String("Team", t.Name))
	close(t.stopChan)
	t.sync.mainSync.Wait()
	close(t.RequestChannel)
	for i := 0; i < len(t.workers); i++ {
		t.Logger.Debug("Stopping Worker",
			zap.String("Team", t.Name),
			zap.Int("Worker", i),
		)
		close(t.workers[i])
	}
	t.sync.workerSync.Wait()
	t.Logger.Debug("Stop Workers Complete", zap.String("Team", t.Name))
}

func (t *Team) listen() {
	defer t.sync.mainSync.Done()
Loop:
	for {
		select {
		case request := <-t.RequestChannel:
			t.Logger.Debug("Request Received", zap.String("Team", t.Name), zap.String("RequestType", request.ReqType().String()))
			_, consistent := t.requestMap.Consistent[request.ReqType().ID()]
			switch {
			case consistent:
				// Hash to a consistent worker
				t.Logger.Debug("Forwarding Consist Request", zap.String("Team", t.Name), zap.String("RequestType", request.ReqType().String()))
				t.workers[int(xxhash.ChecksumString64(request.ConsistID())%uint64(len(t.workers)))] <- request
			default:
				// Send to any worker
				t.Logger.Debug("Forwarding Request", zap.String("Team", t.Name), zap.String("RequestType", request.ReqType().String()))
				t.workers[int(rand.Int31n(int32(len(t.workers))))] <- request
			}
		case <-t.stopChan:
			t.Logger.Debug("Stopping Listener", zap.String("Team", t.Name))
			break Loop
		}
	}
}

// StartWorker takes a WorkerSetup starts the Work Process.
func (t *Team) startWorker(id int, requestChan chan TaskRequest) {
	t.sync.mainSync.Done()
	defer t.sync.workerSync.Done()
	t.Logger.Debug("Started Worker", zap.String("Team", t.Name), zap.Int("WorkerID", id))
	for request := range requestChan {
		t.Logger.Debug("Received Request", zap.String("Team", t.Name), zap.Int("WorkerID", id), zap.String("RequestType", request.ReqType().String()))
		requestFunc, ok := t.requestMap.All[request.ReqType().ID()]
		switch {
		case ok:
			t.Logger.Debug("Processing Request", zap.String("Team", t.Name), zap.Int("WorkerID", id), zap.String("RequestType", request.ReqType().String()))
			requestFunc(request)
		default:
			t.Logger.Debug("Cannot Process, No Matching Handler", zap.String("Team", t.Name), zap.Int("WorkerID", id), zap.String("RequestType", request.ReqType().String()))
			// Add logging error here: Request Type or Handler not Found.
			if request.ResultChan() != nil {
				close(request.ResultChan())
			}
		}
	}
	t.Logger.Debug("Worker Stopped", zap.String("Team", t.Name), zap.Int("WorkerID", id))
}
