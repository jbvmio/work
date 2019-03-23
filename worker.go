package work

import (
	"sync"
)

// WorkerID is the ID assigned to each worker.
// Should be Unique for each Worker.
// type WorkerID int

// WorkerSetup contains the core components for a Worker.
type WorkerSetup struct {
	WorkerID       int
	RequestChannel chan *Request
	RequestMap     HandleRequestMap
	Sync           *sync.WaitGroup
}

// Worker recieves work Requests.
type Worker interface {
	Setup() *WorkerSetup
}

// StartWorker takes a WorkerSetup starts the Work Process.
func StartWorker(setup *WorkerSetup) {
	defer setup.Sync.Done()
	for request := range setup.RequestChannel {
		r := *request
		requestFunc, ok := setup.RequestMap[r.Type()]
		switch {
		case ok:
			requestFunc(request)
		default:
			// Add logging error here: Request Type or Handler not Found.
			if r.Response() != nil {
				close(r.Response())
			}
		}
	}
}
