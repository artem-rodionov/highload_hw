package strategy

import (
	"context"
	"testing"
	"time"

	"golang/internal/adapter"
	"golang/internal/entity"

	"github.com/stretchr/testify/require"
)

func TestHedgingStrategy_FirstSuccessDoesNotHedge(t *testing.T) {
	first := adapter.NewSuccessSender()
	second := adapter.NewSuccessSender()
	strat := NewHedgingStrategy(50 * time.Millisecond)
	budget := entity.Budget{TotalLatency: 200 * time.Millisecond}

	start := time.Now()
	resp, err := strat.Execute(context.Background(), adapter.Request{}, []adapter.SubrequestSender{first, second}, budget)

	require.NoError(t, err)
	require.Equal(t, adapter.OutcomeSuccess, resp.Outcome)
	require.Less(t, time.Since(start), 50*time.Millisecond, "success on the first sender must return before hedging")
	require.Equal(t, 1, first.Calls())
	require.Equal(t, 0, second.Calls(), "second sender must not be touched")
}

func TestHedgingStrategy_HedgesAfterDelay(t *testing.T) {
	first := adapter.NewTimeoutSender()
	second := adapter.NewSuccessSender()
	strat := NewHedgingStrategy(20 * time.Millisecond)
	budget := entity.Budget{TotalLatency: 300 * time.Millisecond}

	start := time.Now()
	resp, err := strat.Execute(context.Background(), adapter.Request{}, []adapter.SubrequestSender{first, second}, budget)
	elapsed := time.Since(start)

	require.NoError(t, err)
	require.Equal(t, adapter.OutcomeSuccess, resp.Outcome)
	require.GreaterOrEqual(t, elapsed, 15*time.Millisecond, "hedge must wait for hedgeDelay")
	require.Less(t, elapsed, budget.TotalLatency)
	require.Eventually(t, func() bool { return second.Calls() == 1 }, time.Second, time.Millisecond)
}

func TestHedgingStrategy_LaunchesAllRemainingAfterDelay(t *testing.T) {
	first := adapter.NewTimeoutSender()
	second := adapter.NewTimeoutSender()
	third := adapter.NewSuccessSender()
	strat := NewHedgingStrategy(20 * time.Millisecond)
	budget := entity.Budget{TotalLatency: 300 * time.Millisecond}

	resp, err := strat.Execute(context.Background(), adapter.Request{}, []adapter.SubrequestSender{first, second, third}, budget)

	require.NoError(t, err)
	require.Equal(t, adapter.OutcomeSuccess, resp.Outcome)
	require.Eventually(t, func() bool { return second.Calls() == 1 }, time.Second, time.Millisecond,
		"the second sender must be launched together with the third")
}

func TestHedgingStrategy_AllSendersHangUntilLatencyBudget(t *testing.T) {
	first := adapter.NewTimeoutSender()
	second := adapter.NewTimeoutSender()
	strat := NewHedgingStrategy(10 * time.Millisecond)
	budget := entity.Budget{TotalLatency: 50 * time.Millisecond}

	start := time.Now()
	resp, err := strat.Execute(context.Background(), adapter.Request{}, []adapter.SubrequestSender{first, second}, budget)
	elapsed := time.Since(start)

	require.ErrorIs(t, err, entity.ErrBudgetExhausted)
	require.Equal(t, adapter.OutcomeTimeout, resp.Outcome)
	require.GreaterOrEqual(t, elapsed, 40*time.Millisecond, "must wait for the latency budget")
	require.Less(t, elapsed, time.Second)
}

func TestHedgingStrategy_AllSendersFailButWaitsForLatencyBudget(t *testing.T) {
	first := adapter.NewServerErrorSender()
	second := adapter.NewServerErrorSender()
	strat := NewHedgingStrategy(10 * time.Millisecond)
	budget := entity.Budget{TotalLatency: 50 * time.Millisecond}

	start := time.Now()
	resp, err := strat.Execute(context.Background(), adapter.Request{}, []adapter.SubrequestSender{first, second}, budget)
	elapsed := time.Since(start)

	require.ErrorIs(t, err, entity.ErrBudgetExhausted)
	require.Equal(t, adapter.OutcomeServerError, resp.Outcome)
	require.GreaterOrEqual(t, elapsed, 40*time.Millisecond, "failed hedge still waits for the whole budget")
}

func TestHedgingStrategy_ValidatesInput(t *testing.T) {
	t.Run("empty pool", func(t *testing.T) {
		strat := NewHedgingStrategy(10 * time.Millisecond)
		budget := entity.Budget{TotalLatency: 100 * time.Millisecond}

		_, err := strat.Execute(context.Background(), adapter.Request{}, nil, budget)

		require.ErrorIs(t, err, entity.ErrNoSenders)
	})

	t.Run("missing total latency", func(t *testing.T) {
		strat := NewHedgingStrategy(10 * time.Millisecond)
		senders := []adapter.SubrequestSender{adapter.NewSuccessSender()}

		_, err := strat.Execute(context.Background(), adapter.Request{}, senders, entity.Budget{})

		require.ErrorIs(t, err, entity.ErrInvalidBudget)
	})
}
