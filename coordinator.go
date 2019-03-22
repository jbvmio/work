package work

import (
	"math/rand"
	"sync"

	"github.com/OneOfOne/xxhash"
)

// Default Values
const (
	workerCount = 3
	queueDepth  = 1
)

// Setup contains the configuration details and components needed.
type Setup struct {
	WorkerCount    int
	QueueDepth     int
	HashRequests   HandleRequestMap
	Running        *sync.WaitGroup
	WorkerRunning  *sync.WaitGroup
	Workers        []chan *Request
	RequestChannel chan *Request
}

// NewSetup returns a new Setup with Defaults.
func NewSetup() *Setup {
	return &Setup{
		WorkerCount:    workerCount,
		QueueDepth:     queueDepth,
		Running:        &sync.WaitGroup{},
		WorkerRunning:  &sync.WaitGroup{},
		Workers:        make([]chan *Request, workerCount),
		RequestChannel: make(chan *Request, queueDepth),
	}
}

// Coordinator Handles Requests and Assigns Workers.
type Coordinator interface {
	Setup() *Setup
}

// Start here.
func Start(coord Coordinator, workers []Worker) {
	for i := 0; i < coord.Setup().WorkerCount; i++ {
		coord.Setup().Workers[i] = make(chan *Request, coord.Setup().QueueDepth)
		coord.Setup().WorkerRunning.Add(1)
		go Work(workers[i])
	}
	coord.Setup().Running.Add(1)
	go coordLoop(coord.Setup())
}

// Stop here.
func Stop(coord Coordinator) {
	close(coord.Setup().RequestChannel)
	coord.Setup().WorkerRunning.Wait()
	for i := 0; i < coord.Setup().WorkerCount; i++ {
		close(coord.Setup().Workers[i])
	}
	coord.Setup().WorkerRunning.Wait()
}

func coordLoop(setup *Setup) {
	defer setup.Running.Done()
	for request := range setup.RequestChannel {
		r := *request
		_, consistent := setup.HashRequests[r.Type()]
		switch {
		case consistent:
			// Hash to a consistent worker
			setup.Workers[int(xxhash.ChecksumString64(r.String())%uint64(setup.WorkerCount))] <- request
		case !consistent:
			// Send to any worker
			setup.Workers[int(rand.Int31n(int32(setup.WorkerCount)))] <- request
		default:
		}
	}
}
