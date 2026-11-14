package adapter

import "errors"

var (
	ErrClient = errors.New("client error")
	ErrServer = errors.New("server error")
)
