package work

import (
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/OneOfOne/xxhash"
)

// TeamConfig contains configuration values for a work Team.
type TeamConfig struct {
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
		Workers:         10,
		WorkerQueueSize: 20,
		MaxTimeSecs:     1,
		CloseOnTimeout:  true,
	}
}

// Team is the coordinating members performing work.
type Team struct {
	Config         *TeamConfig
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

// NewTeam creates a new team with the number of workers specified.
func NewTeam(config *TeamConfig) *Team {
	if config == nil {
		config = NewTeamConfig()
	}
	return &Team{
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
		t.sync.mainSync.Done()
		return true
	case <-timeout:
		fmt.Println("TIMED OUT!!!", request.RequestType())
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
	for i := 0; i < len(t.workers); i++ {
		t.workers[i] = make(chan TaskRequest, t.Config.WorkerQueueSize)
		t.sync.mainSync.Add(1)
		t.sync.workerSync.Add(1)
		go startWorker(i, t.workers[i], t.requestMap.All, &t.sync)
	}
	// wait for all workers to start
	t.sync.mainSync.Wait()
	// add workerSync for listener
	t.sync.mainSync.Add(1)
	go t.listen()
}

// Stop here.
func (t *Team) Stop() {
	fmt.Println("Stop Recieved.")
	close(t.stopChan)
	t.sync.mainSync.Wait()
	close(t.RequestChannel)
	for i := 0; i < len(t.workers); i++ {
		fmt.Println("Stopping Worker", i)
		close(t.workers[i])
	}
	t.sync.workerSync.Wait()
	fmt.Println("Stop Done.")
}

func (t *Team) listen() {
	//t.sync.mainSync.Add(1)
	defer t.sync.mainSync.Done()
	//defer t.sync.workerSync.Done()
Loop:
	for {
		select {
		case request := <-t.RequestChannel:
			//t.sync.mainSync.Add(1)
			_, consistent := t.requestMap.Consistent[request.RequestType()] //t.consistencyMap[request.RequestType()]
			switch {
			case consistent:
				// Hash to a consistent worker
				t.workers[int(xxhash.ChecksumString64(`consist`+strconv.Itoa(int(request.RequestType())))%uint64(len(t.workers)))] <- request
			default:
				// Send to any worker
				t.workers[int(rand.Int31n(int32(len(t.workers))))] <- request
			}
			//t.sync.mainSync.Done()
		case <-t.stopChan:
			break Loop
		}
	}
}

// StartWorker takes a WorkerSetup starts the Work Process.
func startWorker(id int, requestChan chan TaskRequest, requestMap RequestMapping, sync *syncGroup) {
	sync.mainSync.Done()
	defer sync.workerSync.Done()
	fmt.Println("Started Worker", id)
	for request := range requestChan {
		fmt.Println("Worker", id, "Received Request", request.RequestType())
		requestFunc, ok := requestMap[request.RequestType()]
		switch {
		case ok:
			fmt.Println("Worker", id, "OK")
			requestFunc(request)
		default:
			fmt.Println("Worker", id, "NOT OK")
			// Add logging error here: Request Type or Handler not Found.
			if request.ResultChan() != nil {
				close(request.ResultChan())
			}
		}
	}
	fmt.Println("Worker", id, "Stopped")
}
