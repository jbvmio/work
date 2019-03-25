package work

import (
	"fmt"
	"time"
)

// Result represents any result or response from a Request.
type Result interface{}

// NewResultChannel is a convenience func returning a new Result channel.
func NewResultChannel() chan Result {
	return make(chan Result, 1)
}

// RequestTypeID is used to determine the type of Request to be serviced by a RequestHandler.
type RequestTypeID int

// Data represents any data or value related for a Request.
type Data interface{}

// TaskRequest represents a work Request.
type TaskRequest interface {
	RequestType() RequestTypeID
	ResultChan() chan Result
	Get() Data
}

// Request implements TaskRequest.
type Request struct {
	// RequestTypeID for the Request. Used to determine the appropriate RequestHandler.
	RequestTypeID RequestTypeID

	// Data contains any data or value related for the Request.
	Data Data

	// An overriding RequestHandleFunc for this Request instance.
	// If nil, the corresponding RequestTypeID RequestHandleFunc will be used.
	Handler RequestHandleFunc

	// Result channel can used for any Result or response expected from a RequestHandler if desired.
	// If the Result channel is expected from the Request, the ResultChan should be created when making the Request.
	// The Result channel should be closed when no longer needed.
	Result chan Result
}

// RequestType returns the RequestTypeID of the Request.
func (r *Request) RequestType() RequestTypeID {
	return r.RequestTypeID
}

// ResultChan returns the underlying Result channel of the Request.
// If a Response is expected from the Request, the ResponseChannel should be created when making the Request.
func (r *Request) ResultChan() chan Result {
	return r.Result
}

// Get returns any Data in the Request.
func (r *Request) Get() Data {
	return r.Data
}

// GetResult returns the Result of the Request and closes the Result channel.
// Returns nil if the channel is closed or if the attempt times out with the given maxTime in seconds.
// GetResult will block if using an unbuffered Result channel until either the Result is available or timeout occurs.
func (r *Request) GetResult(maxTime int) Result {
	if maxTime < 1 {
		maxTime = 1
	}
	timeout := time.After(time.Duration(maxTime) * time.Second)
	if r.Result == nil {
		fmt.Println("NIL 1")
		return nil
	}
	select {
	case i, ok := <-r.Result:
		fmt.Println("i, ok >", i, ok)
		switch {
		case ok:
			fmt.Println("RETURNING RESULT")
			close(r.Result)
			return i
		}
	case <-timeout:
		fmt.Println("TIMED OUT")
		break
	}
	fmt.Println("NIL 4")
	return nil
}

// RequestHandleFunc handles Requests.
// The RequestHandleFunc is responsible for handling any Responses required using the Reply channel.
type RequestHandleFunc func(TaskRequest)

// NoopHandler can be used to as default RequestHandler.
// It does nothing with the request and closes the Result channel as needed.
func NoopHandler(request TaskRequest) {
	if request.ResultChan() != nil {
		close(request.ResultChan())
	}
}

// RequestMapping is a mapping of RequestConstants and RequestHandlers.
type RequestMapping map[RequestTypeID]RequestHandleFunc

// NewRequestMapping returns an empty HandleRequestMap.
func NewRequestMapping() RequestMapping {
	return make(map[RequestTypeID]RequestMapping)
}

// RequestMap contains both Consistent Mappings and All Mappings.
type RequestMap struct {
	Consistent RequestMapping
	All        RequestMapping
}

// NewRequestMap returns a RequestMap with both Consistent and All Maps.
func NewRequestMap() {
	return RequestMap{
		Consistent: NewRequestMapping(),
		All:        NewRequestMapping(),
	}
}

// SendRequest sends a Request to a channel with a timeout specified in seconds.
// If the request is sent, return true. Otherwise, if the timeout is hit, return false.
// A Listener should be available to service the request.
func SendRequest(requestChannel chan TaskRequest, request TaskRequest, maxTime int) bool {
	timeout := time.After(time.Duration(maxTime) * time.Second)
	select {
	case requestChannel <- request:
		return true
	case <-timeout:
		return false
	}
}
