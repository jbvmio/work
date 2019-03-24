package work

import (
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/OneOfOne/xxhash"
)

// Team is the coordinating members performing work.
type Team struct {
	RequestChannel chan TaskRequest
	workers        []chan TaskRequest
	requestMap     HandleRequestMap
	consistencyMap HandleRequestMap
	stopChan       chan struct{}
	sync           syncGroup
}

type syncGroup struct {
	mainSync   *sync.WaitGroup
	workerSync *sync.WaitGroup
}

// NewTeam creates a new team with the number of workers specified.
func NewTeam(workers int) *Team {
	return &Team{
		RequestChannel: make(chan TaskRequest, workers),
		workers:        make([]chan TaskRequest, workers),
		requestMap:     NewRequestMap(),
		consistencyMap: NewRequestMap(),
		sync: syncGroup{
			mainSync:   &sync.WaitGroup{},
			workerSync: &sync.WaitGroup{},
		},
		stopChan: make(chan struct{}),
	}
}

// AddTask adds a RequestTypeID and the corresponding RequestHandler for the request.
func (t *Team) AddTask(id RequestTypeID, requestFunc RequestHandleFunc) {
	t.requestMap[id] = requestFunc
}

// SendRequest sends a Request to the Team with a timeout specified in seconds.
// If the request is sent, return true. Otherwise, if the timeout is hit, return false.
func (t *Team) SendRequest(request TaskRequest, maxTime int) bool {
	timeout := time.After(time.Duration(maxTime) * time.Second)
	select {
	case t.RequestChannel <- request:
		return true
	case <-timeout:
		return false
	}
}

// Start starts all workers and Listner.
func (t *Team) Start() {
	for i := 0; i < len(t.workers); i++ {
		t.workers[i] = make(chan TaskRequest) //, queueDepth)
		t.sync.mainSync.Add(1)
		t.sync.workerSync.Add(1)
		go startWorker(i, t.workers[i], t.requestMap, &t.sync)
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

// StartWorker takes a WorkerSetup starts the Work Process.
func startWorker(id int, requestChan chan TaskRequest, requestMap HandleRequestMap, sync *syncGroup) {
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
			if request.ResponseChan() != nil {
				close(request.ResponseChan())
			}
		}
	}
	fmt.Println("Worker", id, "Stopped")
}

func (t *Team) listen() {
	//t.sync.mainSync.Add(1)
	defer t.sync.mainSync.Done()
	//defer t.sync.workerSync.Done()
Loop:
	for {
		select {
		case request := <-t.RequestChannel:
			t.sync.mainSync.Add(1)
			_, consistent := t.consistencyMap[request.RequestType()]
			switch {
			case consistent:
				// Hash to a consistent worker
				t.workers[int(xxhash.ChecksumString64(`consist`+strconv.Itoa(int(request.RequestType())))%uint64(len(t.workers)))] <- request
			default:
				// Send to any worker
				t.workers[int(rand.Int31n(int32(len(t.workers))))] <- request
			}
			t.sync.mainSync.Done()
		case <-t.stopChan:
			break Loop
		}
	}
}
