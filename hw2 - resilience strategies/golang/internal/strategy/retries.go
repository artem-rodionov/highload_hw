package strategy

import (
	"context"
	"time"

	"golang/internal/adapter"
	"golang/internal/entity"
)

type retryStrategy struct{}

func NewRetryStrategy() Strategy {
	return &retryStrategy{}
}

func (s *retryStrategy) Execute(ctx context.Context, req adapter.Request, sender adapter.SubrequestSender, budget entity.Budget) (adapter.Response, error) {
	startTime := time.Now()

	maxAttempts := budget.MaxRetries
	if maxAttempts <= 0 {
		maxAttempts = 1
	}

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

		resp, err := executeSubrequest(ctx, req, sender, timeoutBudget)
		attempts++
		last = resp

		if ctx.Err() != nil {
			return adapter.Response{Outcome: adapter.OutcomeUnknown}, ctx.Err()
		}

		if resp.Outcome == adapter.OutcomeTimeout {
			timeouts++
		} else if err != nil {
			return resp, err
		} else {
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
