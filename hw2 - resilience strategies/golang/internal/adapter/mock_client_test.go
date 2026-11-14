package adapter

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMockSender_AlwaysSenders(t *testing.T) {
	tests := []struct {
		name    string
		sender  *MockSender
		outcome Outcome
	}{
		{"success", NewSuccessSender(), OutcomeSuccess},
		{"client error", NewClientErrorSender(), OutcomeClientError},
		{"server error", NewServerErrorSender(), OutcomeServerError},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for range 3 {
				resp, err := tt.sender.Send(context.Background(), Request{})
				require.NoError(t, err)
				require.Equal(t, tt.outcome, resp.Outcome)
			}
			require.Equal(t, 3, tt.sender.Calls())
		})
	}
}

func TestMockSender_TimeoutHangsUntilCanceled(t *testing.T) {
	sender := NewTimeoutSender()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	start := time.Now()
	resp, err := sender.Send(ctx, Request{})

	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, OutcomeTimeout, resp.Outcome)
	require.Less(t, time.Since(start), time.Second, "sender must not wait the whole hang duration")
}

func TestMockSender_ScriptedSequence(t *testing.T) {
	sender := NewScriptedSender(
		ServerError().WithDelay(time.Millisecond),
		ClientError(),
		Success(),
	)

	expected := []Outcome{OutcomeServerError, OutcomeClientError, OutcomeSuccess}
	for _, want := range expected {
		resp, err := sender.Send(context.Background(), Request{})
		require.NoError(t, err)
		require.Equal(t, want, resp.Outcome)
	}
}

func TestMockSender_EmptyScenarioAnswersSuccess(t *testing.T) {
	sender := &MockSender{}

	resp, err := sender.Send(context.Background(), Request{})
	require.NoError(t, err)
	require.Equal(t, OutcomeSuccess, resp.Outcome)
}

func TestMockSender_ConcurrentCalls(t *testing.T) {
	sender := NewScriptedOutcomes(OutcomeSuccess, OutcomeServerError, OutcomeClientError)

	const goroutines = 50
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for range goroutines {
		go func() {
			defer wg.Done()
			_, _ = sender.Send(context.Background(), Request{})
		}()
	}
	wg.Wait()

	require.Equal(t, goroutines, sender.Calls())
}
