package adapter

import (
	"context"
	"sync"
	"time"
)

const hangDuration = time.Hour

type Step struct {
	outcome Outcome
	delay   time.Duration
	hang    bool
}

func Success() Step { return Step{outcome: OutcomeSuccess} }

func ClientError() Step { return Step{outcome: OutcomeClientError} }

func ServerError() Step { return Step{outcome: OutcomeServerError} }

func Timeout() Step { return Step{outcome: OutcomeTimeout, hang: true} }

func (s Step) WithDelay(d time.Duration) Step {
	if s.hang {
		return s
	}
	s.delay = d
	return s
}

type MockSender struct {
	mu sync.Mutex

	Responses []Response
	Delays    []time.Duration

	CallCount int

	fallbackResponse *Response
	fallbackDelay    time.Duration
}

func NewSuccessSender() *MockSender { return NewAlwaysSender(OutcomeSuccess) }

func NewClientErrorSender() *MockSender { return NewAlwaysSender(OutcomeClientError) }

func NewServerErrorSender() *MockSender { return NewAlwaysSender(OutcomeServerError) }

func NewTimeoutSender() *MockSender { return NewAlwaysSender(OutcomeTimeout) }

func NewAlwaysSender(outcome Outcome) *MockSender {
	resp := Response{Outcome: outcome}
	m := &MockSender{fallbackResponse: &resp}
	if outcome == OutcomeTimeout {
		m.fallbackDelay = hangDuration
	}
	return m
}

func NewScriptedSender(steps ...Step) *MockSender {
	m := &MockSender{}
	for _, s := range steps {
		delay := s.delay
		if s.hang {
			delay = hangDuration
		}
		m.Responses = append(m.Responses, Response{Outcome: s.outcome})
		m.Delays = append(m.Delays, delay)
	}
	return m
}

func NewScriptedOutcomes(outcomes ...Outcome) *MockSender {
	steps := make([]Step, 0, len(outcomes))
	for _, outcome := range outcomes {
		steps = append(steps, Step{outcome: outcome})
	}
	return NewScriptedSender(steps...)
}

func (m *MockSender) Calls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.CallCount
}

func (m *MockSender) Send(ctx context.Context, req Request) (Response, error) {
	delay, resp := m.next()

	if delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()

		select {
		case <-timer.C:
		case <-ctx.Done():
			return Response{Outcome: OutcomeTimeout}, ctx.Err()
		}
	}

	if err := ctx.Err(); err != nil {
		return Response{Outcome: OutcomeTimeout}, err
	}

	return resp, nil
}

func (m *MockSender) next() (time.Duration, Response) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.CallCount++

	delay := m.fallbackDelay
	if len(m.Delays) > 0 {
		delay = m.Delays[0]
		m.Delays = m.Delays[1:]
	}

	var resp Response
	switch {
	case len(m.Responses) > 0:
		resp = m.Responses[0]
		m.Responses = m.Responses[1:]
	case m.fallbackResponse != nil:
		resp = *m.fallbackResponse
	default:
		resp = Response{Outcome: OutcomeSuccess}
	}

	return delay, resp
}
