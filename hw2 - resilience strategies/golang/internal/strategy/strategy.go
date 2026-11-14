package strategy

import (
	"context"

	"golang/internal/adapter"
	"golang/internal/entity"
)

type Strategy interface {
	Execute(ctx context.Context, req adapter.Request, sender adapter.SubrequestSender, budget entity.Budget) (adapter.Response, error)
}

type MultiStrategy interface {
	Execute(ctx context.Context, req adapter.Request, senders []adapter.SubrequestSender, budget entity.Budget) (adapter.Response, error)
}
