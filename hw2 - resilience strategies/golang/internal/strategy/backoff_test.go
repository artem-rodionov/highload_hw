package strategy

import (
	"context"
	"testing"
	"time"

	"golang/internal/adapter"
	"golang/internal/entity"

	"github.com/stretchr/testify/require"
)

func TestExponentialBackoffStrategy_SuccessOnFirstAttempt(t *testing.T) {
	sender := adapter.NewScriptedSender(adapter.Success())
	strat := NewExponentialBackoffStrategy(time.Hour)
	budget := entity.Budget{SubrequestLatency: time.Second}

	resp, err := strat.Execute(context.Background(), adapter.Request{}, sender, budget)

	require.NoError(t, err)
	require.Equal(t, adapter.OutcomeSuccess, resp.Outcome)
	require.Equal(t, 1, sender.Calls())
}

func TestExponentialBackoffStrategy_GrowsDelayBetweenServerErrors(t *testing.T) {
	budget := entity.Budget{MaxRetries: 10, SubrequestLatency: time.Second}
	sender := adapter.NewScriptedSender(
		adapter.ServerError(),
		adapter.ServerError(),
		adapter.Success(),
	)
	strat := NewExponentialBackoffStrategy(10 * time.Millisecond)

	start := time.Now()
	resp, err := strat.Execute(context.Background(), adapter.Request{}, sender, budget)
	elapsed := time.Since(start)

	require.NoError(t, err)
	require.Equal(t, adapter.OutcomeSuccess, resp.Outcome)
	require.Equal(t, 3, sender.Calls())
	require.GreaterOrEqual(t, elapsed, 30*time.Millisecond)
}

func TestExponentialBackoffStrategy_DoesNotDelayClientErrors(t *testing.T) {
	budget := entity.Budget{MaxRetries: 10, FastErrors: 5, SubrequestLatency: time.Second}
	sender := adapter.NewScriptedSender(
		adapter.ClientError(),
		adapter.Success(),
	)
	strat := NewExponentialBackoffStrategy(300 * time.Millisecond)

	start := time.Now()
	resp, err := strat.Execute(context.Background(), adapter.Request{}, sender, budget)
	elapsed := time.Since(start)

	require.NoError(t, err)
	require.Equal(t, adapter.OutcomeSuccess, resp.Outcome)
	require.Equal(t, 2, sender.Calls())
	require.Less(t, elapsed, 200*time.Millisecond)
}

func TestExponentialBackoffStrategy_DelaysTimeoutRetries(t *testing.T) {
	budget := entity.Budget{MaxRetries: 10, Timeouts: 5, SubrequestLatency: 10 * time.Millisecond}
	sender := adapter.NewScriptedSender(
		adapter.Timeout(),
		adapter.Success(),
	)
	strat := NewExponentialBackoffStrategy(10 * time.Millisecond)

	start := time.Now()
	resp, err := strat.Execute(context.Background(), adapter.Request{}, sender, budget)
	elapsed := time.Since(start)

	require.NoError(t, err)
	require.Equal(t, adapter.OutcomeSuccess, resp.Outcome)
	require.Equal(t, 2, sender.Calls())
	require.GreaterOrEqual(t, elapsed, 20*time.Millisecond)
}

func TestExponentialBackoffStrategy_StopsWhenFailuresBudgetIsExhausted(t *testing.T) {
	budget := entity.Budget{MaxRetries: 10, Failures: 2, SubrequestLatency: time.Second}
	sender := adapter.NewServerErrorSender()
	strat := NewExponentialBackoffStrategy(time.Millisecond)

	resp, err := strat.Execute(context.Background(), adapter.Request{}, sender, budget)

	require.ErrorIs(t, err, entity.ErrBudgetExhausted)
	require.Equal(t, adapter.OutcomeServerError, resp.Outcome)
	require.Equal(t, 2, sender.Calls())
}

func TestExponentialBackoffStrategy_TotalLatencyClampsDelay(t *testing.T) {
	budget := entity.Budget{
		MaxRetries:        10,
		TotalLatency:      20 * time.Millisecond,
		SubrequestLatency: time.Second,
	}
	sender := adapter.NewServerErrorSender()
	strat := NewExponentialBackoffStrategy(time.Second)

	start := time.Now()
	_, err := strat.Execute(context.Background(), adapter.Request{}, sender, budget)
	elapsed := time.Since(start)

	require.ErrorIs(t, err, entity.ErrBudgetExhausted)
	require.Less(t, elapsed, 500*time.Millisecond)
	require.GreaterOrEqual(t, elapsed, 15*time.Millisecond)
	require.GreaterOrEqual(t, sender.Calls(), 1)
}
