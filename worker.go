package work

/*
// WorkerID is the ID assigned to each worker.
// Should be Unique for each Worker.
// type WorkerID int

// WorkerSetup contains the core components for a Worker.
type WorkerSetup struct {
	WorkerID       int
	RequestChannel chan Request
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
	fmt.Println("Started Worker", setup.WorkerID, setup.RequestMap)
	for request := range setup.RequestChannel {
		fmt.Println("Worker", setup.WorkerID, "Received Request", request.Type())
		//r := request
		requestFunc, ok := setup.RequestMap[request.Type()]
		switch {
		case ok:
			fmt.Println("Worker", setup.WorkerID, "OK")
			requestFunc(request)
		default:
			fmt.Println("Worker", setup.WorkerID, "NOT OK")
			// Add logging error here: Request Type or Handler not Found.
			if request.Response() != nil {
				close(request.Response())
			}
		}
	}
	fmt.Println("Worker", setup.WorkerID, "Stopped")
}
*/
