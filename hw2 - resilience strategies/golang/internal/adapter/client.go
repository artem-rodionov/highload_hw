package adapter

import "context"

type SubrequestSender interface {
	Send(ctx context.Context, req Request) (Response, error)
}
