package strategy

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"golang/internal/adapter"
	"golang/internal/entity"

	"github.com/stretchr/testify/require"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Unix(0, 0)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func newTestCircuit(t *testing.T, cfg CircuitBreakerConfig, clock *fakeClock) MultiStrategy {
	t.Helper()
	strat, err := newCircuitBreakingStrategy(cfg, clock.Now)
	require.NoError(t, err)
	return strat
}

func TestCircuitBreakingStrategy_OpensAndSkipsFailedSender(t *testing.T) {
	clock := newFakeClock()
	strat := newTestCircuit(t, CircuitBreakerConfig{
		WindowSize:       2,
		FailureThreshold: 0.5,
		OpenTimeout:      50 * time.Millisecond,
	}, clock)

	broken := adapter.NewScriptedSender(adapter.ServerError(), adapter.ServerError())
	healthy := adapter.NewSuccessSender()
	senders := []adapter.SubrequestSender{broken, healthy}
	budget := entity.Budget{MaxRetries: 1, SubrequestLatency: time.Second}

	_, err := strat.Execute(context.Background(), adapter.Request{}, senders, budget)
	require.ErrorIs(t, err, entity.ErrBudgetExhausted)

	resp, err := strat.Execute(context.Background(), adapter.Request{}, senders, budget)
	require.NoError(t, err)
	require.Equal(t, adapter.OutcomeSuccess, resp.Outcome)

	_, err = strat.Execute(context.Background(), adapter.Request{}, senders, budget)
	require.ErrorIs(t, err, entity.ErrBudgetExhausted)
	require.Equal(t, 2, broken.Calls())

	resp, err = strat.Execute(context.Background(), adapter.Request{}, senders, budget)
	require.NoError(t, err)
	require.Equal(t, adapter.OutcomeSuccess, resp.Outcome)
	require.Equal(t, 2, broken.Calls())
	require.Equal(t, 2, healthy.Calls())
}

func TestCircuitBreakingStrategy_HalfOpenSuccessClosesCircuit(t *testing.T) {
	clock := newFakeClock()
	strat := newTestCircuit(t, CircuitBreakerConfig{
		WindowSize:       1,
		FailureThreshold: 0.5,
		OpenTimeout:      50 * time.Millisecond,
	}, clock)

	sender := adapter.NewScriptedSender(adapter.ServerError(), adapter.Success())
	senders := []adapter.SubrequestSender{sender}
	budget := entity.Budget{MaxRetries: 1, SubrequestLatency: time.Second}

	_, err := strat.Execute(context.Background(), adapter.Request{}, senders, budget)
	require.ErrorIs(t, err, entity.ErrBudgetExhausted)
	require.Equal(t, 1, sender.Calls())

	_, err = strat.Execute(context.Background(), adapter.Request{}, senders, budget)
	require.ErrorIs(t, err, entity.ErrCircuitOpen)
	require.Equal(t, 1, sender.Calls())

	clock.Advance(50 * time.Millisecond)
	resp, err := strat.Execute(context.Background(), adapter.Request{}, senders, budget)
	require.NoError(t, err)
	require.Equal(t, adapter.OutcomeSuccess, resp.Outcome)
	require.Equal(t, 2, sender.Calls())

	resp, err = strat.Execute(context.Background(), adapter.Request{}, senders, budget)
	require.NoError(t, err)
	require.Equal(t, adapter.OutcomeSuccess, resp.Outcome)
	require.Equal(t, 3, sender.Calls())
}

func TestCircuitBreakingStrategy_HalfOpenFailureReopensCircuit(t *testing.T) {
	clock := newFakeClock()
	strat := newTestCircuit(t, CircuitBreakerConfig{
		WindowSize:       1,
		FailureThreshold: 0.5,
		OpenTimeout:      50 * time.Millisecond,
	}, clock)

	sender := adapter.NewServerErrorSender()
	senders := []adapter.SubrequestSender{sender}
	budget := entity.Budget{MaxRetries: 1, SubrequestLatency: time.Second}

	_, err := strat.Execute(context.Background(), adapter.Request{}, senders, budget)
	require.ErrorIs(t, err, entity.ErrBudgetExhausted)

	_, err = strat.Execute(context.Background(), adapter.Request{}, senders, budget)
	require.ErrorIs(t, err, entity.ErrCircuitOpen)

	clock.Advance(50 * time.Millisecond)
	_, err = strat.Execute(context.Background(), adapter.Request{}, senders, budget)
	require.ErrorIs(t, err, entity.ErrBudgetExhausted, "trial failure must be returned as a request failure")
	require.Equal(t, 2, sender.Calls())

	_, err = strat.Execute(context.Background(), adapter.Request{}, senders, budget)
	require.ErrorIs(t, err, entity.ErrCircuitOpen, "failed trial must reopen the circuit")
}

func TestCircuitBreakingStrategy_ClientErrorAndTimeoutOpenCircuit(t *testing.T) {
	tests := []struct {
		name   string
		sender *adapter.MockSender
		budget entity.Budget
	}{
		{
			name:   "client error",
			sender: adapter.NewClientErrorSender(),
			budget: entity.Budget{MaxRetries: 1, SubrequestLatency: time.Second},
		},
		{
			name:   "timeout",
			sender: adapter.NewTimeoutSender(),
			budget: entity.Budget{MaxRetries: 1, SubrequestLatency: 5 * time.Millisecond},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clock := newFakeClock()
			strat := newTestCircuit(t, CircuitBreakerConfig{
				WindowSize:       1,
				FailureThreshold: 0.5,
				OpenTimeout:      50 * time.Millisecond,
			}, clock)
			senders := []adapter.SubrequestSender{tt.sender}

			_, err := strat.Execute(context.Background(), adapter.Request{}, senders, tt.budget)
			require.ErrorIs(t, err, entity.ErrBudgetExhausted)

			_, err = strat.Execute(context.Background(), adapter.Request{}, senders, tt.budget)
			require.ErrorIs(t, err, entity.ErrCircuitOpen)
		})
	}
}

func TestCircuitBreakingStrategy_TransportErrorOpensCircuit(t *testing.T) {
	clock := newFakeClock()
	strat := newTestCircuit(t, CircuitBreakerConfig{
		WindowSize:       1,
		FailureThreshold: 0.5,
		OpenTimeout:      50 * time.Millisecond,
	}, clock)

	sender := &countingErrorSender{err: errors.New("connection reset")}
	senders := []adapter.SubrequestSender{sender}
	budget := entity.Budget{MaxRetries: 1, SubrequestLatency: time.Second}

	_, err := strat.Execute(context.Background(), adapter.Request{}, senders, budget)
	require.ErrorIs(t, err, entity.ErrBudgetExhausted)

	_, err = strat.Execute(context.Background(), adapter.Request{}, senders, budget)
	require.ErrorIs(t, err, entity.ErrCircuitOpen)
	require.Equal(t, 1, sender.Calls())
}

func TestCircuitBreakingStrategy_RejectsInvalidConfig(t *testing.T) {
	tests := []CircuitBreakerConfig{
		{WindowSize: 0, FailureThreshold: 0.5, OpenTimeout: time.Second},
		{WindowSize: 1, FailureThreshold: 0, OpenTimeout: time.Second},
		{WindowSize: 1, FailureThreshold: 1.5, OpenTimeout: time.Second},
		{WindowSize: 1, FailureThreshold: 0.5, OpenTimeout: 0},
	}

	for _, cfg := range tests {
		_, err := NewCircuitBreakingStrategy(cfg)
		require.ErrorIs(t, err, entity.ErrInvalidConfig)
	}

	_, err := NewCircuitBreakingStrategy(CircuitBreakerConfig{
		WindowSize:       1,
		FailureThreshold: 0.5,
		OpenTimeout:      time.Second,
	})
	require.NoError(t, err)
}
