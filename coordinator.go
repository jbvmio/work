package work

import (
	"fmt"
	"math/rand"
	"sync"

	"github.com/OneOfOne/xxhash"
)

// Default Values
const (
	defaultWorkerCount = 3
	defualtQueueDepth  = 1
)

// Setup contains the core components for the Coordinator.
type Setup struct {
	WorkerCount    int
	QueueDepth     int
	HashRequests   HandleRequestMap
	Sync           *sync.WaitGroup
	WorkerSync     *sync.WaitGroup
	Workers        []chan Request
	RequestChannel chan Request
}

// NewSetup returns a new Setup with default values which can be modified.
func NewSetup() *Setup {
	return &Setup{
		WorkerCount: defaultWorkerCount,
		QueueDepth:  defualtQueueDepth,
		Sync:        &sync.WaitGroup{},
		WorkerSync:  &sync.WaitGroup{},
	}
}

// Coordinator Handles Requests and Assigns Workers.
type Coordinator interface {
	Setup() *Setup
}

// StartWork creates the specified number of Workers, assigns the given HandleRequestMap and starts the work process.
func StartWork(coord Coordinator, requestMap HandleRequestMap) {
	coord.Setup().Workers = make([]chan Request, coord.Setup().WorkerCount)
	coord.Setup().RequestChannel = make(chan Request, coord.Setup().QueueDepth)
	for i := 0; i < coord.Setup().WorkerCount; i++ {
		coord.Setup().Workers[i] = make(chan Request, coord.Setup().QueueDepth)
		setup := WorkerSetup{
			WorkerID:       i,
			RequestChannel: coord.Setup().Workers[i],
			RequestMap:     requestMap,
			Sync:           coord.Setup().WorkerSync,
		}
		coord.Setup().WorkerSync.Add(1)
		go StartWorker(&setup)
	}
	coord.Setup().Sync.Add(1)
	go coordLoop(coord.Setup())
}

// StartWorkers takes pre-configured Workers and starts the work process.
func StartWorkers(coord Coordinator, workers []Worker) {
	for i := 0; i < len(workers); i++ {
		coord.Setup().WorkerSync.Add(1)
		go StartWorker(workers[i].Setup())
	}
	coord.Setup().Sync.Add(1)
	go coordLoop(coord.Setup())
}

// Stop here.
func Stop(coord Coordinator) {
	fmt.Println("Stop Recieved.")
	close(coord.Setup().RequestChannel)
	for i := 0; i < coord.Setup().WorkerCount; i++ {
		fmt.Println("Stopping Worker", i)
		close(coord.Setup().Workers[i])
	}
	coord.Setup().WorkerSync.Wait()
	fmt.Println("Stop Done.")
}

func coordLoop(setup *Setup) {
	defer setup.Sync.Done()
	for request := range setup.RequestChannel {
		//r := *request
		_, consistent := setup.HashRequests[request.Type()]
		switch {
		case consistent:
			// Hash to a consistent worker
			setup.Workers[int(xxhash.ChecksumString64(request.ID())%uint64(setup.WorkerCount))] <- request
		default:
			// Send to any worker
			setup.Workers[int(rand.Int31n(int32(setup.WorkerCount)))] <- request
		}
	}
}
