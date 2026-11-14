package strategy

import (
	"context"
	"math"
	"time"

	"golang/internal/adapter"
	"golang/internal/entity"
)

type exponentialBackoffStrategy struct {
	baseDelay time.Duration
}

func NewExponentialBackoffStrategy(baseDelay time.Duration) Strategy {
	return &exponentialBackoffStrategy{baseDelay: baseDelay}
}

func (s *exponentialBackoffStrategy) Execute(ctx context.Context, req adapter.Request, sender adapter.SubrequestSender, budget entity.Budget) (adapter.Response, error) {
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

		var currentDelay time.Duration

		if resp.Outcome == adapter.OutcomeTimeout {
			timeouts++
			currentDelay = s.calculateDelay(attempts)
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
				currentDelay = s.calculateDelay(attempts)
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

		if currentDelay > 0 {
			if budget.TotalLatency > 0 {
				remaining := budget.TotalLatency - time.Since(startTime)
				if remaining <= 0 {
					break
				}
				if currentDelay > remaining {
					currentDelay = remaining
				}
			}

			select {
			case <-ctx.Done():
				return last, ctx.Err()
			case <-time.After(currentDelay):
			}
		}
	}

	return last, entity.ErrBudgetExhausted
}

func (s *exponentialBackoffStrategy) calculateDelay(attempts int) time.Duration {
	if attempts <= 0 {
		return 0
	}
	return time.Duration(float64(s.baseDelay) * math.Pow(2, float64(attempts-1)))
}
