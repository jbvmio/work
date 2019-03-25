package work

import (
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/OneOfOne/xxhash"
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
	RequestChannel chan TaskRequest
	Logger         Logger
	workers        []chan TaskRequest
	requestMap     RequestMap
	stopChan       chan struct{}
	sync           syncGroup
}

type syncGroup struct {
	mainSync   *sync.WaitGroup
	workerSync *sync.WaitGroup
}

// NewTeam creates a new team with the number of workers specified.
func NewTeam(config *TeamConfig) *Team {
	if config == nil {
		config = NewTeamConfig()
	}
	return &Team{
		Name:           config.Name,
		Config:         config,
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
func (t *Team) AddTask(id RequestTypeID, requestFunc RequestHandleFunc) {
	t.requestMap.All[id] = requestFunc
}

// AddConsist is the same as AddTask with the exception that the RequestTypeID should be serviced by a consistent worker.
func (t *Team) AddConsist(id RequestTypeID, requestFunc RequestHandleFunc) {
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
		t.Logger.Debug("Submit Request Successful", LogWith("Team", t.Name), LogWith("RequestType", request.RequestType()))
		t.sync.mainSync.Done()
		return true
	case <-timeout:
		t.Logger.Debug("Submit Request TIMED OUT", LogWith("Team", t.Name), LogWith("RequestType", request.RequestType()))
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
	t.configureLogger()
	for i := 0; i < len(t.workers); i++ {
		t.workers[i] = make(chan TaskRequest, t.Config.WorkerQueueSize)
		t.sync.mainSync.Add(1)
		t.sync.workerSync.Add(1)
		t.Logger.Debug("Starting Worker", LogWith("Team", t.Name), LogWith("WorkerID", i))
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
	t.Logger.Debug("Stop Received.", LogWith("Team", t.Name))
	t.Logger.Debug("Stopping", LogWith("Team", t.Name))
	close(t.stopChan)
	t.sync.mainSync.Wait()
	close(t.RequestChannel)
	for i := 0; i < len(t.workers); i++ {
		t.Logger.Debug("Stopping Worker",
			LogWith("Team", t.Name),
			LogWith("Worker", i),
		)
		close(t.workers[i])
	}
	t.sync.workerSync.Wait()
	t.Logger.Debug("Stop Workers Complete", LogWith("Team", t.Name))
}

func (t *Team) listen() {
	defer t.sync.mainSync.Done()
Loop:
	for {
		select {
		case request := <-t.RequestChannel:
			t.Logger.Debug("Request Received", LogWith("Team", t.Name), LogWith("RequestType", request.RequestType()))
			_, consistent := t.requestMap.Consistent[request.RequestType()]
			switch {
			case consistent:
				// Hash to a consistent worker
				t.Logger.Debug("Sending Request to Consistent Worker", LogWith("Team", t.Name), LogWith("RequestType", request.RequestType()))
				t.workers[int(xxhash.ChecksumString64(`consist`+strconv.Itoa(int(request.RequestType())))%uint64(len(t.workers)))] <- request
			default:
				// Send to any worker
				t.Logger.Debug("Sending Request to Worker", LogWith("Team", t.Name), LogWith("RequestType", request.RequestType()))
				t.workers[int(rand.Int31n(int32(len(t.workers))))] <- request
			}
		case <-t.stopChan:
			t.Logger.Debug("Stopping Listener", LogWith("Team", t.Name))
			break Loop
		}
	}
}

// StartWorker takes a WorkerSetup starts the Work Process.
func (t *Team) startWorker(id int, requestChan chan TaskRequest) { //, requestMap RequestMapping, sync *syncGroup) {
	t.sync.mainSync.Done()
	defer t.sync.workerSync.Done()
	t.Logger.Debug("Started Worker", LogWith("Team", t.Name), LogWith("WorkerID", id))
	for request := range requestChan {
		t.Logger.Debug("Received Request", LogWith("Team", t.Name), LogWith("WorkerID", id), LogWith("RequestType", request.RequestType()))
		requestFunc, ok := t.requestMap.All[request.RequestType()]
		switch {
		case ok:
			t.Logger.Debug("Processing Request", LogWith("Team", t.Name), LogWith("WorkerID", id), LogWith("RequestType", request.RequestType()))
			requestFunc(request)
		default:
			t.Logger.Debug("Cannot Process, No Matching Handler", LogWith("Team", t.Name), LogWith("WorkerID", id), LogWith("RequestType", request.RequestType()))
			// Add logging error here: Request Type or Handler not Found.
			if request.ResultChan() != nil {
				close(request.ResultChan())
			}
		}
	}
	t.Logger.Debug("Worker Stopped", LogWith("Team", t.Name), LogWith("WorkerID", id))
}

// Start starts all workers and Listner.
func (t *Team) configureLogger() {
	if t.Logger == nil {
		var noopLogger Logger = newNoopLogger()
		t.Logger = noopLogger
	}
	switch t.Logger.(type) {
	case *DefaultLogger:
		logger := t.Logger.(*DefaultLogger)
		if logger.Prefix() == "" {
			switch {
			case t.Name != "Default":
				logger.SetPrefix("[" + t.Name + "] ")
			case t.Config.Name != "Default":
				logger.SetPrefix(string("[Config " + t.Config.Name + "] "))
			case t.Name == "Default" || t.Config.Name == "Default":
				logger.SetPrefix(string("[Default] "))
			default:
				logger.SetPrefix(string("[UnKnownTeam] "))
			}
		}
	}
}
