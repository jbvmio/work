package work

import "time"

// Object is the interface which represents any data.
type Object interface {
	// ID returns an identifying string for the object.
	ID() string
}

// Request represents a work Request.
type Request interface {
	Object
	// GetObject returns the underlying Object
	GetObject() Object
	// Type returns the RequestConstant of the Request.
	Type() RequestConstant
	// GetReplyChannel returns the underlying Response channel.
	Response() chan Response
}

// Response contains the response from a Request
type Response interface {
	Object
	// GetObject returns the underlying Object
	GetObject() Object
	// Failed return false if there were any failures encountered during the work process.
	Err() error
	// HasObject should return true if Response contains a valid Object.
	HasObject() bool
}

// RequestConstant is used in Request to indicate the type of request. Numeric ordering is not important.
type RequestConstant int

// RequestHandler handles an all purpose Request.
// The RequestHandler is responsible for handling any Responses required using the Reply channel.
type RequestHandler func(Request)

// NoopHandler can be used to as default RequestHandler.
// It does nothing with the request and closes the Reply channel as needed.
func NoopHandler(request Request) {
	if request.Response() != nil {
		close(request.Response())
	}
}

// HandleRequestMap is a mapping of RequestConstants and RequestHandlers.
type HandleRequestMap map[RequestConstant]RequestHandler

// NewRequestMap returns an empty HandleRequestMap.
func NewRequestMap() HandleRequestMap {
	return make(map[RequestConstant]RequestHandler)
}

// SendRequest sends a Request to a channel with a timeout specified in seconds.
// If the request is sent, return true. Otherwise, if the timeout is hit, return false.
// A Listener should be available to service the request.
func SendRequest(requestChannel chan Request, request Request, maxTime int) bool {
	timeout := time.After(time.Duration(maxTime) * time.Second)
	select {
	case requestChannel <- request:
		return true
	case <-timeout:
		return false
	}
}
