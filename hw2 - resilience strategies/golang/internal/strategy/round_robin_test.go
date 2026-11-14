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

type countingErrorSender struct {
	mu    sync.Mutex
	err   error
	calls int
}

func (s *countingErrorSender) Send(_ context.Context, _ adapter.Request) (adapter.Response, error) {
	s.mu.Lock()
	s.calls++
	s.mu.Unlock()
	return adapter.Response{Outcome: adapter.OutcomeUnknown}, s.err
}

func (s *countingErrorSender) Calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func TestRoundRobinStrategy_RotatesAcrossCalls(t *testing.T) {
	senders := []adapter.SubrequestSender{
		adapter.NewSuccessSender(),
		adapter.NewSuccessSender(),
		adapter.NewSuccessSender(),
	}
	strat := NewRoundRobinStrategy()
	budget := entity.Budget{MaxRetries: 1, SubrequestLatency: time.Second}

	for i := 0; i < 6; i++ {
		resp, err := strat.Execute(context.Background(), adapter.Request{}, senders, budget)
		require.NoError(t, err)
		require.Equal(t, adapter.OutcomeSuccess, resp.Outcome)
	}

	for i, sender := range senders {
		require.Equal(t, 2, sender.(*adapter.MockSender).Calls(), "sender %d", i)
	}
}

func TestRoundRobinStrategy_RetriesAdvanceRingWithinOneCall(t *testing.T) {
	senders := []adapter.SubrequestSender{
		adapter.NewScriptedSender(adapter.ServerError(), adapter.Success()),
		adapter.NewServerErrorSender(),
	}
	strat := NewRoundRobinStrategy()
	budget := entity.Budget{MaxRetries: 3, SubrequestLatency: time.Second}

	resp, err := strat.Execute(context.Background(), adapter.Request{}, senders, budget)

	require.NoError(t, err)
	require.Equal(t, adapter.OutcomeSuccess, resp.Outcome)
	require.Equal(t, 2, senders[0].(*adapter.MockSender).Calls())
	require.Equal(t, 1, senders[1].(*adapter.MockSender).Calls())
}

func TestRoundRobinStrategy_StopsWhenFailuresBudgetIsExhausted(t *testing.T) {
	sender0 := adapter.NewServerErrorSender()
	sender1 := adapter.NewServerErrorSender()
	sender2 := adapter.NewServerErrorSender()
	senders := []adapter.SubrequestSender{sender0, sender1, sender2}
	strat := NewRoundRobinStrategy()
	budget := entity.Budget{MaxRetries: 10, Failures: 2, SubrequestLatency: time.Second}

	resp, err := strat.Execute(context.Background(), adapter.Request{}, senders, budget)

	require.ErrorIs(t, err, entity.ErrBudgetExhausted)
	require.Equal(t, adapter.OutcomeServerError, resp.Outcome)
	require.Equal(t, 1, sender0.Calls())
	require.Equal(t, 1, sender1.Calls())
	require.Equal(t, 0, sender2.Calls())
}

func TestRoundRobinStrategy_TransportErrorMovesToNextSender(t *testing.T) {
	boom := errors.New("connection reset")
	sender0 := &countingErrorSender{err: boom}
	sender1 := &countingErrorSender{err: boom}
	senders := []adapter.SubrequestSender{sender0, sender1}
	strat := NewRoundRobinStrategy()
	budget := entity.Budget{MaxRetries: 2, SubrequestLatency: time.Second}

	_, err := strat.Execute(context.Background(), adapter.Request{}, senders, budget)

	require.ErrorIs(t, err, entity.ErrBudgetExhausted)
	require.Equal(t, 1, sender0.Calls())
	require.Equal(t, 1, sender1.Calls())
}

func TestRoundRobinStrategy_EmptyPoolIsRejected(t *testing.T) {
	strat := NewRoundRobinStrategy()
	budget := entity.Budget{MaxRetries: 1, SubrequestLatency: time.Second}

	_, err := strat.Execute(context.Background(), adapter.Request{}, nil, budget)

	require.ErrorIs(t, err, entity.ErrNoSenders)
}

func TestRoundRobinStrategy_ConcurrentCallsDistributeEvenly(t *testing.T) {
	senders := []adapter.SubrequestSender{
		adapter.NewSuccessSender(),
		adapter.NewSuccessSender(),
		adapter.NewSuccessSender(),
	}
	strat := NewRoundRobinStrategy()
	budget := entity.Budget{MaxRetries: 1, SubrequestLatency: time.Second}

	const workers = 30
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			resp, err := strat.Execute(context.Background(), adapter.Request{}, senders, budget)
			require.NoError(t, err)
			require.Equal(t, adapter.OutcomeSuccess, resp.Outcome)
		}()
	}
	wg.Wait()

	for i, sender := range senders {
		require.Equal(t, workers/len(senders), sender.(*adapter.MockSender).Calls(), "sender %d", i)
	}
}
