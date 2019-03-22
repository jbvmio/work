package work

import (
	"sync"
)

// WorkerID is the ID assigned to each worker.
// Should be Unique for each Worker.
type WorkerID int

// Worker recieves work Requests.
type Worker interface {
	ID() WorkerID
	Requests() chan *Request
	RequestMap() HandleRequestMap
	WorkerRunning() *sync.WaitGroup
}

// Work starts a Worker Process.
func Work(worker Worker) {
	defer worker.WorkerRunning().Done()
	for request := range worker.Requests() {
		r := *request
		requestFunc, ok := worker.RequestMap()[r.Type()]
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
