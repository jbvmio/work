package work

import (
	"sync"
)

// Listener listens on the Request channel and routes requests.
type Listener interface {
	Sync() *sync.WaitGroup
	RequestChannel() chan Request
}

// TaskListener listens and forwards requests to workers.
type TaskListener struct {

}

// Listen takes a TaskListener and starts the listen process.
func Listen(l Listener) {
	defer l.Sync().Done()
Loop:
	for {
		select {
		case request := <-l.RequestChannel():
			//t.sync.mainSync.Add(1)
			_, consistent := t.consistencyMap[request.RequestType()]
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