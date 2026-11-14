package strategy

import (
	"context"
	"fmt"
	"sync"
	"time"

	"golang/internal/adapter"
	"golang/internal/entity"
)

type CircuitBreakerConfig struct {
	WindowSize       int
	FailureThreshold float64
	OpenTimeout      time.Duration
}

type breakerState struct {
	window    []bool
	pos       int
	filled    int
	openUntil time.Time
	trial     bool
}

func newBreakerState(size int) *breakerState {
	return &breakerState{window: make([]bool, size)}
}

func (s *breakerState) reset() {
	for i := range s.window {
		s.window[i] = false
	}
	s.pos = 0
	s.filled = 0
}

func (s *breakerState) push(failed bool) {
	s.window[s.pos] = failed
	s.pos = (s.pos + 1) % len(s.window)
	if s.filled < len(s.window) {
		s.filled++
	}
}

func (s *breakerState) failures() int {
	count := 0
	for _, failed := range s.window {
		if failed {
			count++
		}
	}
	return count
}

type circuitBreakingStrategy struct {
	cfg CircuitBreakerConfig
	now func() time.Time

	mu     sync.Mutex
	next   uint64
	states map[adapter.SubrequestSender]*breakerState
}

func NewCircuitBreakingStrategy(cfg CircuitBreakerConfig) (MultiStrategy, error) {
	return newCircuitBreakingStrategy(cfg, time.Now)
}

func newCircuitBreakingStrategy(cfg CircuitBreakerConfig, now func() time.Time) (*circuitBreakingStrategy, error) {
	if cfg.WindowSize <= 0 {
		return nil, fmt.Errorf("%w: window size must be positive", entity.ErrInvalidConfig)
	}
	if cfg.FailureThreshold <= 0 || cfg.FailureThreshold > 1 {
		return nil, fmt.Errorf("%w: failure threshold must be in (0, 1]", entity.ErrInvalidConfig)
	}
	if cfg.OpenTimeout <= 0 {
		return nil, fmt.Errorf("%w: open timeout must be positive", entity.ErrInvalidConfig)
	}
	if now == nil {
		now = time.Now
	}
	return &circuitBreakingStrategy{
		cfg:    cfg,
		now:    now,
		states: make(map[adapter.SubrequestSender]*breakerState),
	}, nil
}

func (s *circuitBreakingStrategy) Execute(ctx context.Context, req adapter.Request, senders []adapter.SubrequestSender, budget entity.Budget) (adapter.Response, error) {
	if err := validateSenders(senders); err != nil {
		return adapter.Response{Outcome: adapter.OutcomeUnknown}, err
	}

	maxAttempts := budget.MaxRetries
	if maxAttempts <= 0 {
		maxAttempts = 1
	}

	startTime := time.Now()

	fastErrors := 0
	failures := 0
	timeouts := 0
	attempts := 0
	var last adapter.Response

	for attempts < maxAttempts {
		elapsed := time.Since(startTime)
		if budget.TotalLatency > 0 && elapsed >= budget.TotalLatency {
			break
		}

		sender, state, trial, ok := s.pick(senders)
		if !ok {
			if attempts == 0 {
				return adapter.Response{Outcome: adapter.OutcomeUnknown}, entity.ErrCircuitOpen
			}
			return last, entity.ErrCircuitOpen
		}

		timeoutBudget := budget
		if budget.TotalLatency > 0 {
			timeoutBudget.TotalLatency = budget.TotalLatency - elapsed
		}

		resp, err := executeSubrequest(ctx, req, sender, timeoutBudget)
		attempts++
		last = resp

		s.record(state, trial, resp, err)

		if ctx.Err() != nil {
			return adapter.Response{Outcome: adapter.OutcomeUnknown}, ctx.Err()
		}

		switch {
		case resp.Outcome == adapter.OutcomeTimeout:
			timeouts++
		case err != nil:
			failures++
		default:
			switch resp.Outcome {
			case adapter.OutcomeSuccess:
				return resp, nil
			case adapter.OutcomeClientError:
				fastErrors++
			case adapter.OutcomeServerError:
				failures++
			default:
				return resp, entity.ErrUnknownOutcome
			}
		}

		if budget.FastErrors > 0 && fastErrors >= budget.FastErrors {
			break
		}
		if budget.Failures > 0 && failures >= budget.Failures {
			break
		}
		if budget.Timeouts > 0 && timeouts >= budget.Timeouts {
			break
		}
	}

	return last, entity.ErrBudgetExhausted
}

func (s *circuitBreakingStrategy) pick(senders []adapter.SubrequestSender) (adapter.SubrequestSender, *breakerState, bool, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	n := len(senders)
	start := int(s.next % uint64(n))
	s.next++
	now := s.now()

	for i := 0; i < n; i++ {
		sender := senders[(start+i)%n]
		state := s.states[sender]
		if state == nil {
			state = newBreakerState(s.cfg.WindowSize)
			s.states[sender] = state
		}

		halfOpen := !state.openUntil.IsZero()
		if halfOpen && now.Before(state.openUntil) {
			continue
		}
		if halfOpen {
			if state.trial {
				continue
			}
			state.trial = true
			return sender, state, true, true
		}

		return sender, state, false, true
	}

	return nil, nil, false, false
}

func (s *circuitBreakingStrategy) record(state *breakerState, trial bool, resp adapter.Response, err error) {
	failed := err != nil || resp.Outcome != adapter.OutcomeSuccess

	s.mu.Lock()
	defer s.mu.Unlock()

	if trial {
		state.trial = false
		state.reset()
		if failed {
			state.openUntil = s.now().Add(s.cfg.OpenTimeout)
			return
		}
		state.openUntil = time.Time{}
		return
	}

	state.push(failed)
	if state.filled < s.cfg.WindowSize {
		return
	}

	ratio := float64(state.failures()) / float64(s.cfg.WindowSize)
	if ratio > s.cfg.FailureThreshold {
		state.openUntil = s.now().Add(s.cfg.OpenTimeout)
		state.reset()
	}
}
