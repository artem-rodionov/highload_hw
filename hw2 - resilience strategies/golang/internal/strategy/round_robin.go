package strategy

import (
	"context"
	"sync/atomic"
	"time"

	"golang/internal/adapter"
	"golang/internal/entity"
)

type roundRobinStrategy struct {
	next atomic.Uint64
}

func NewRoundRobinStrategy() MultiStrategy {
	return &roundRobinStrategy{}
}

func (s *roundRobinStrategy) Execute(ctx context.Context, req adapter.Request, senders []adapter.SubrequestSender, budget entity.Budget) (adapter.Response, error) {
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

		timeoutBudget := budget
		if budget.TotalLatency > 0 {
			timeoutBudget.TotalLatency = budget.TotalLatency - elapsed
		}

		idx := (s.next.Add(1) - 1) % uint64(len(senders))
		sender := senders[idx]

		resp, err := executeSubrequest(ctx, req, sender, timeoutBudget)

		attempts++
		last = resp

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

func validateSenders(senders []adapter.SubrequestSender) error {
	if len(senders) == 0 {
		return entity.ErrNoSenders
	}
	for _, sender := range senders {
		if sender == nil {
			return entity.ErrNoSenders
		}
	}
	return nil
}
