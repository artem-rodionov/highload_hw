package strategy

import (
	"context"
	"errors"

	"golang/internal/adapter"
	"golang/internal/entity"
)

type defaultStrategy struct{}

func NewDefaultStrategy() Strategy {
	return &defaultStrategy{}
}

func (s *defaultStrategy) Execute(ctx context.Context, req adapter.Request, sender adapter.SubrequestSender, budget entity.Budget) (adapter.Response, error) {
	return executeSubrequest(ctx, req, sender, budget)
}

func executeSubrequest(ctx context.Context, req adapter.Request, sender adapter.SubrequestSender, budget entity.Budget) (adapter.Response, error) {
	timeout := budget.SubrequestLatency
	if budget.TotalLatency > 0 && (budget.TotalLatency < timeout || timeout == 0) {
		timeout = budget.TotalLatency
	}

	subCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	resp, err := sender.Send(subCtx, req)
	if errors.Is(subCtx.Err(), context.DeadlineExceeded) {
		return adapter.Response{Outcome: adapter.OutcomeTimeout}, context.DeadlineExceeded
	}
	return resp, err
}
