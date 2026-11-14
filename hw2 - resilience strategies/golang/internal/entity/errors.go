package entity

import "errors"

var (
	ErrBudgetExhausted = errors.New("budget exhausted")
	ErrUnknownOutcome  = errors.New("unknown outcome")
	ErrNoSenders       = errors.New("no senders")
	ErrCircuitOpen     = errors.New("circuit open")
	ErrInvalidBudget   = errors.New("invalid budget")
	ErrInvalidConfig   = errors.New("invalid config")
)
