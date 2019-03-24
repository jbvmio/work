package work

import (
	"time"
)

/*
// Request represents a work Request.
type Request interface {
	// RequestType returns the RequestTypeID the Request.
	RequestType() RequestTypeID
	// Response returns any Replies returned by a RequestHandler.
	Response() chan interface{}
}
*/

// Response represents any reply from a Request.
type Response interface{}

// RequestTypeID is used to determine the type of Request to be serviced by a RequestHandler.
type RequestTypeID int

// TaskRequest represents a work Request.
type TaskRequest interface {
	RequestType() RequestTypeID
	GetResponse() Response
	ResponseChan() chan Response
}

// Request implements TaskRequest.
type Request struct {
	// RequestTypeID for the Request. Used to determine the appropriate RequestHandler.
	RequestTypeID RequestTypeID

	// Data contains anything related for the Request.
	Data interface{}

	// An overriding RequestHandleFunc for this Request instance.
	// If nil, the corresponding RequestTypeID RequestHandleFunc will be used.
	Handler RequestHandleFunc

	// ResponseChan is the channel made for any replies expected from a RequestHandler.
	// If a Response is expected from the Request, the ResponseChannel should be created when making the Request.
	Response chan Response
}

// RequestType returns the RequestTypeID of the Request.
func (r *Request) RequestType() RequestTypeID {
	return r.RequestTypeID
}

// ResponseChan returns the underlying Response Channel of the Request.
// If a Response is expected from the Request, the ResponseChannel should be created when making the Request.
func (r *Request) ResponseChan() chan Response {
	return r.Response
}

// GetResponse returns the Response if the Request currently contains one and closes the Response channel.
// Returns nil otherwise.
func (r *Request) GetResponse() Response {
	if r.Response == nil {
		return nil
	}
	select {
	case i, ok := <-r.Response:
		switch {
		case ok:
			close(r.Response)
			return i
		default:
			//Channel Closed
			return nil
		}
	default:
		return nil
	}
	return nil
}

// RequestHandleFunc handles Requests.
// The RequestHandleFunc is responsible for handling any Responses required using the Reply channel.
type RequestHandleFunc func(TaskRequest)

// NoopHandler can be used to as default RequestHandler.
// It does nothing with the request and closes the Reply channel as needed.
func NoopHandler(request TaskRequest) {
	if request.ResponseChan() != nil {
		close(request.ResponseChan())
	}
}

// HandleRequestMap is a mapping of RequestConstants and RequestHandlers.
type HandleRequestMap map[RequestTypeID]RequestHandleFunc

// NewRequestMap returns an empty HandleRequestMap.
func NewRequestMap() HandleRequestMap {
	return make(map[RequestTypeID]RequestHandleFunc)
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
