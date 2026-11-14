package strategy

import (
	"context"
	"time"

	"golang/internal/adapter"
	"golang/internal/entity"
)

type hedgingStrategy struct {
	hedgeDelay time.Duration
}

func NewHedgingStrategy(hedgeDelay time.Duration) MultiStrategy {
	return &hedgingStrategy{hedgeDelay: hedgeDelay}
}

type subrequestResult struct {
	resp adapter.Response
	err  error
}

func (s *hedgingStrategy) Execute(ctx context.Context, req adapter.Request, senders []adapter.SubrequestSender, budget entity.Budget) (adapter.Response, error) {
	if err := validateSenders(senders); err != nil {
		return adapter.Response{Outcome: adapter.OutcomeUnknown}, err
	}
	if budget.TotalLatency <= 0 {
		return adapter.Response{Outcome: adapter.OutcomeUnknown}, entity.ErrInvalidBudget
	}

	parentCtx := ctx
	ctx, cancel := context.WithTimeout(ctx, budget.TotalLatency)
	defer cancel()

	results := make(chan subrequestResult, len(senders))

	launch := func(sender adapter.SubrequestSender) {
		go func() {
			resp, err := executeSubrequest(ctx, req, sender, budget)
			results <- subrequestResult{resp: resp, err: err}
		}()
	}

	launched := 1
	launch(senders[0])

	var hedgeC <-chan time.Time
	if len(senders) > 1 {
		if s.hedgeDelay <= 0 {
			for launched < len(senders) {
				launch(senders[launched])
				launched++
			}
		} else {
			timer := time.NewTimer(s.hedgeDelay)
			defer timer.Stop()
			hedgeC = timer.C
		}
	}

	var last adapter.Response

	for {
		select {
		case result := <-results:
			if result.err == nil && result.resp.Outcome == adapter.OutcomeSuccess {
				cancel()
				return result.resp, nil
			}
			last = result.resp
		case <-hedgeC:
			for launched < len(senders) {
				launch(senders[launched])
				launched++
			}
			hedgeC = nil
		case <-ctx.Done():
			if parentCtx.Err() != nil {
				return adapter.Response{Outcome: adapter.OutcomeUnknown}, parentCtx.Err()
			}
			if last.Outcome == adapter.OutcomeUnknown {
				last.Outcome = adapter.OutcomeTimeout
			}
			return last, entity.ErrBudgetExhausted
		}
	}
}
