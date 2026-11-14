package entity

import "time"

type Budget struct {
	MaxRetries        int
	FastErrors        int
	Failures          int
	Timeouts          int
	TotalLatency      time.Duration
	SubrequestLatency time.Duration
}
