package work

import (
	"math/rand"
	"strconv"
	"sync"

	"github.com/OneOfOne/xxhash"
)

// Listener listens on the Request channel and routes requests.
type Listener interface {
	Sync() *sync.WaitGroup
	RequestChannel() chan Request
	StopChannel() chan struct{}
}

// TaskListener listens and forwards requests to workers.
type TaskListener struct {
}

// Listen takes a TaskListener array of TaskRequest channels and starts the listen process.
func Listen(l Listener, workers []chan TaskRequest, requestMap RequestMap) {
	defer l.Sync().Done()
Loop:
	for {
		select {
		case request := <-l.RequestChannel():
			_, consistent := requestMap.Consistent[request.RequestType()]
			switch {
			case consistent:
				// Hash to a consistent worker
				workers[int(xxhash.ChecksumString64(`consist`+strconv.Itoa(int(request.RequestType())))%uint64(len(workers)))] <- &request
			default:
				// Send to any worker
				workers[int(rand.Int31n(int32(len(workers))))] <- &request
			}
		case <-l.StopChannel():
			break Loop
		}
	}
}
