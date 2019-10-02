package rateLimit

import (
	"time"

	"github.com/Pushwoosh/go-connection-pool/pkg/message"
)

type (
	RateLimitingAlgorithm int
	TimeGetter            func() int64
)

const (
	TokenBucketAlgorithm RateLimitingAlgorithm = iota
)

type Config struct {
	Algorithm  RateLimitingAlgorithm
	TimeGetter TimeGetter    // if you want use faster implementation
	WaitTime   time.Duration // if you want reduce CPU usage
	Rate       int64         // Rate means messages number per second
}

type TokenBucket struct {
	config    Config
	tokens    int64
	timestamp int64
}

func NewRateLimiter(c Config) RateLimiter {
	if nil == c.TimeGetter {
		c.TimeGetter = func() int64 { return time.Now().Unix() }
	}
	var r RateLimiter
	// nolint: gocritic
	switch c.Algorithm {
	case TokenBucketAlgorithm:
		r = &TokenBucket{config: c}
	}

	return r
}

// https://en.wikipedia.org/wiki/Token_bucket
func (t *TokenBucket) Serve(in chan message.Message, out chan message.Message) error {
	t.tokens = t.config.Rate
	t.timestamp = t.config.TimeGetter()
	var delta int64
	for m := range in {
	MARK:
		delta = t.config.Rate * (t.config.TimeGetter() - t.timestamp)
		t.tokens = min(t.config.Rate, t.tokens+delta)
		t.timestamp = t.config.TimeGetter()

		if t.tokens < 1 {
			time.Sleep(t.config.WaitTime)
			goto MARK
		}

		t.tokens--
		out <- m
	}
	return nil
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
