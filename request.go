package work

import (
	"fmt"
	"time"
)

// Result represents any result or response from a Request.
type Result interface{}

// NewResultChannel is a convenience func returning a new Result channel.
func NewResultChannel() chan interface{} {
	return make(chan interface{}, 1)
}

// RequestType is used to determine the type of Request to be serviced by a RequestHandler.
type RequestType interface {
	ID() int
	String() string
}

// Data represents any data or value related for a Request.
type Data interface{}

// TaskRequest represents a work Request.
type TaskRequest interface {
	ReqType() RequestType
	ResultChan() chan interface{}
	Get() interface{}
	ConsistID() string
}

// RequestID implements RequestType.
type RequestID struct {
	id           int
	name         string
	consistGroup string
}

// ID returns the Request ID.
func (r *RequestID) ID() int {
	return r.id
}

// String returns the string representation of the Request ID.
func (r *RequestID) String() string {
	return r.name
}

// Request implements TaskRequest.
type Request struct {
	// RequestTypeID for the Request. Used to determine the appropriate RequestHandler.
	RequestType RequestID

	// Data contains any data or value related for the Request.
	Data interface{}

	// Result channel can used for any Result or response expected from a RequestHandler if desired.
	// If the Result channel is expected from the Request, the ResultChan should be created when making the Request.
	// The Result channel should be closed when no longer needed.
	Result chan interface{}
}

// ReqType returns the RequestID of the Request.
func (r *Request) ReqType() RequestType {
	return &r.RequestType
}

// ID returns the ID of the Request.
func (r *Request) ID() int {
	return r.RequestType.ID()
}

// String returns the string representation of the Request ID.
func (r *Request) String() string {
	return r.RequestType.String()
}

// ResultChan returns the underlying Result channel of the Request.
// If a Response is expected from the Request, the ResponseChannel should be created when making the Request.
func (r *Request) ResultChan() chan interface{} {
	return r.Result
}

// Get returns any Data in the Request.
func (r *Request) Get() interface{} {
	return r.Data
}

// ConsistID is any field or identifier in the request that should be used for consistent requests.
// The Request must be added as Consistent first before this will be evaluated.
func (r *Request) ConsistID() string {
	return ""
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
type RequestMapping map[int]RequestHandleFunc

// NewRequestMapping returns an empty HandleRequestMap.
func NewRequestMapping() RequestMapping {
	return make(map[int]RequestHandleFunc)
}

// RequestMap contains both Consistent Mappings and All Mappings.
type RequestMap struct {
	Consistent RequestMapping
	All        RequestMapping
}

// NewRequestMap returns a RequestMap with both Consistent and All Maps.
func NewRequestMap() RequestMap {
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
