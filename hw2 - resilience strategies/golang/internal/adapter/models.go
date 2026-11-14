package adapter

type Outcome int

const (
	OutcomeUnknown Outcome = iota
	OutcomeSuccess
	OutcomeClientError
	OutcomeServerError
	OutcomeTimeout
)

type Response struct {
	Payload []byte
	Outcome Outcome
	Err     error
}

type Request struct {
	Body []byte
}
