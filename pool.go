package work

// Pool is a collection of workers and a Coordinator.
type Pool struct {
	Coord      *Setup
	Workers    []*WorkerSetup
	RequestMap HandleRequestMap
}

// NewPool returns a new Pool.
func NewPool(setup *Setup) Pool {
	return Pool{
		Coord:      setup,
		Workers:    make([]*WorkerSetup, setup.WorkerCount),
		RequestMap: NewRequestMap(),
	}
}

/*
// Setup returns the Pool Coordinator Setup.
func (p *Pool) Setup() *Setup {
	return p.Coord
}

// HashedRequests assigns RequestHandlers that should be services by consistent Workers.
func (p *Pool) HashedRequests(requestMap HandleRequestMap) {
	p.Coord.HashRequests = requestMap
}

// RequestChannel returns the main Request channel.
func (p *Pool) RequestChannel() chan Request {
	return p.Coord.RequestChannel
}

// AddHandler adds RequestHandler.
func (p *Pool) AddHandler(rType RequestConstant, rHandler RequestHandler) {
	p.RequestMap[rType] = rHandler
}

// AddHashed adds a hashed RequestHandler.
func (p *Pool) AddHashed(rType RequestConstant, rHandler RequestHandler) {
	p.Coord.HashRequests[rType] = rHandler
}

// Req requests work.
type Req struct {
	Object
	RequestType  RequestConstant
	responseChan chan Response
}

// NewReq returns a Req with given input.
func NewReq(responseChan chan Response, rType RequestConstant, obj Object) *Req {
	return &Req{
		RequestType:  rType,
		responseChan: responseChan,
		Object:       obj,
	}
}

// Type returns the Request type (RequestConstant).
func (r *Req) Type() RequestConstant {
	return r.RequestType
}

// Response returns the Response channel.
func (r *Req) Response() chan Response {
	return r.responseChan
}

// GetObject returns the underlying Object
func (r *Req) GetObject() Object {
	return r.Object
}

// Res contains the response from a Request
type Res struct {
	Object
	Error  error
	HasObj bool
}

// GetObject returns the underlying Object
func (r *Res) GetObject() Object {
	return r.Object
}

// Err returns any errors encountered during the request/recieve work process.
func (r *Res) Err() error {
	return r.Error
}

// HasObject should return true if the Response contains a valid Object.
func (r *Res) HasObject() bool {
	return r.HasObj
}
*/
