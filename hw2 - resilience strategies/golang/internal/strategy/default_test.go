package strategy

import (
	"context"
	"testing"
	"time"

	"golang/internal/adapter"
	"golang/internal/entity"

	"github.com/stretchr/testify/require"
)

type errorSender struct {
	resp adapter.Response
	err  error
}

func (s errorSender) Send(_ context.Context, _ adapter.Request) (adapter.Response, error) {
	return s.resp, s.err
}

func TestDefaultStrategy_Success(t *testing.T) {
	sender := adapter.NewScriptedSender(
		adapter.Success().WithDelay(time.Millisecond),
	)
	budget := entity.Budget{
		SubrequestLatency: 100 * time.Millisecond,
		TotalLatency:      200 * time.Millisecond,
	}

	resp, err := NewDefaultStrategy().Execute(context.Background(), adapter.Request{}, sender, budget)

	require.NoError(t, err)
	require.Equal(t, adapter.OutcomeSuccess, resp.Outcome)
	require.Equal(t, 1, sender.Calls())
}

func TestDefaultStrategy_ErrorOutcomesAreReturnedAsIs(t *testing.T) {
	tests := []struct {
		name    string
		sender  *adapter.MockSender
		outcome adapter.Outcome
	}{
		{"client error", adapter.NewClientErrorSender(), adapter.OutcomeClientError},
		{"server error", adapter.NewServerErrorSender(), adapter.OutcomeServerError},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			budget := entity.Budget{
				MaxRetries:        5,
				SubrequestLatency: 100 * time.Millisecond,
			}

			resp, err := NewDefaultStrategy().Execute(context.Background(), adapter.Request{}, tt.sender, budget)

			require.NoError(t, err)
			require.Equal(t, tt.outcome, resp.Outcome)
			require.Equal(t, 1, tt.sender.Calls())
		})
	}
}

func TestDefaultStrategy_SubrequestTimeout(t *testing.T) {
	sender := adapter.NewTimeoutSender()
	budget := entity.Budget{SubrequestLatency: 10 * time.Millisecond}

	start := time.Now()
	resp, err := NewDefaultStrategy().Execute(context.Background(), adapter.Request{}, sender, budget)

	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, adapter.OutcomeTimeout, resp.Outcome)
	require.Equal(t, 1, sender.Calls())
	require.Less(t, time.Since(start), time.Second)
}

func TestDefaultStrategy_TotalLatencyCapsSubrequestLatency(t *testing.T) {
	sender := adapter.NewTimeoutSender()
	budget := entity.Budget{
		SubrequestLatency: time.Hour,
		TotalLatency:      10 * time.Millisecond,
	}

	start := time.Now()
	resp, err := NewDefaultStrategy().Execute(context.Background(), adapter.Request{}, sender, budget)

	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, adapter.OutcomeTimeout, resp.Outcome)
	require.Less(t, time.Since(start), time.Second)
}

func TestDefaultStrategy_TotalLatencyIsUsedWhenSubrequestLatencyIsZero(t *testing.T) {
	sender := adapter.NewTimeoutSender()
	budget := entity.Budget{TotalLatency: 10 * time.Millisecond}

	start := time.Now()
	resp, err := NewDefaultStrategy().Execute(context.Background(), adapter.Request{}, sender, budget)

	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, adapter.OutcomeTimeout, resp.Outcome)
	require.Less(t, time.Since(start), time.Second)
}

func TestDefaultStrategy_PropagatesParentCancellation(t *testing.T) {
	sender := adapter.NewTimeoutSender()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	resp, err := NewDefaultStrategy().Execute(ctx, adapter.Request{}, sender, entity.Budget{SubrequestLatency: time.Second})

	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, adapter.OutcomeTimeout, resp.Outcome)
}
