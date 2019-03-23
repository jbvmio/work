package work

// Listener listens on the Request channel and routes requests.
type Listener interface {
	RequestChannel() chan *Request
}
