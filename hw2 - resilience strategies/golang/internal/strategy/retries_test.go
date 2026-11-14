package strategy

import (
	"context"
	"errors"
	"testing"
	"time"

	"golang/internal/adapter"
	"golang/internal/entity"

	"github.com/stretchr/testify/require"
)

func TestRetryStrategy_SuccessOnFirstAttempt(t *testing.T) {
	sender := adapter.NewScriptedSender(adapter.Success())
	budget := entity.Budget{SubrequestLatency: time.Second}

	resp, err := NewRetryStrategy().Execute(context.Background(), adapter.Request{}, sender, budget)

	require.NoError(t, err)
	require.Equal(t, adapter.OutcomeSuccess, resp.Outcome)
	require.Equal(t, 1, sender.Calls())
}

func TestRetryStrategy_RetriesServerErrorUntilSuccess(t *testing.T) {
	budget := entity.Budget{MaxRetries: 10, SubrequestLatency: time.Second}
	sender := adapter.NewScriptedSender(
		adapter.ServerError(),
		adapter.ServerError(),
		adapter.Success(),
	)

	resp, err := NewRetryStrategy().Execute(context.Background(), adapter.Request{}, sender, budget)

	require.NoError(t, err)
	require.Equal(t, adapter.OutcomeSuccess, resp.Outcome)
	require.Equal(t, 3, sender.Calls())
}

func TestRetryStrategy_StopsWhenErrorBudgetIsExhausted(t *testing.T) {
	tests := []struct {
		name      string
		newSender func() *adapter.MockSender
		budget    entity.Budget
		outcome   adapter.Outcome
		wantCalls int
	}{
		{
			name:      "fast errors",
			newSender: adapter.NewClientErrorSender,
			budget:    entity.Budget{MaxRetries: 10, FastErrors: 2, SubrequestLatency: time.Second},
			outcome:   adapter.OutcomeClientError,
			wantCalls: 2,
		},
		{
			name:      "failures",
			newSender: adapter.NewServerErrorSender,
			budget:    entity.Budget{MaxRetries: 10, Failures: 2, SubrequestLatency: time.Second},
			outcome:   adapter.OutcomeServerError,
			wantCalls: 2,
		},
		{
			name:      "timeouts",
			newSender: adapter.NewTimeoutSender,
			budget:    entity.Budget{MaxRetries: 10, Timeouts: 2, SubrequestLatency: 5 * time.Millisecond},
			outcome:   adapter.OutcomeTimeout,
			wantCalls: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sender := tt.newSender()

			resp, err := NewRetryStrategy().Execute(context.Background(), adapter.Request{}, sender, tt.budget)

			require.ErrorIs(t, err, entity.ErrBudgetExhausted)
			require.Equal(t, tt.outcome, resp.Outcome)
			require.Equal(t, tt.wantCalls, sender.Calls())
		})
	}
}

func TestRetryStrategy_StopsAfterMaxRetries(t *testing.T) {
	budget := entity.Budget{MaxRetries: 3, SubrequestLatency: time.Second}
	sender := adapter.NewServerErrorSender()

	resp, err := NewRetryStrategy().Execute(context.Background(), adapter.Request{}, sender, budget)

	require.ErrorIs(t, err, entity.ErrBudgetExhausted)
	require.Equal(t, adapter.OutcomeServerError, resp.Outcome)
	require.Equal(t, 3, sender.Calls())
}

func TestRetryStrategy_TotalLatencyBudgetStopsRetries(t *testing.T) {
	budget := entity.Budget{
		MaxRetries:        10,
		TotalLatency:      30 * time.Millisecond,
		SubrequestLatency: time.Second,
	}
	sender := adapter.NewScriptedSender(
		adapter.ServerError().WithDelay(20*time.Millisecond),
		adapter.ServerError().WithDelay(20*time.Millisecond),
		adapter.Success(),
	)

	start := time.Now()
	_, err := NewRetryStrategy().Execute(context.Background(), adapter.Request{}, sender, budget)

	require.ErrorIs(t, err, entity.ErrBudgetExhausted)
	require.Less(t, time.Since(start), time.Second)
	require.GreaterOrEqual(t, sender.Calls(), 1)
}

func TestRetryStrategy_ReturnsTransportErrorAsIs(t *testing.T) {
	boom := errors.New("connection reset")
	budget := entity.Budget{SubrequestLatency: time.Second}
	sender := errorSender{
		resp: adapter.Response{Outcome: adapter.OutcomeUnknown},
		err:  boom,
	}

	_, err := NewRetryStrategy().Execute(context.Background(), adapter.Request{}, sender, budget)

	require.ErrorIs(t, err, boom)
}
