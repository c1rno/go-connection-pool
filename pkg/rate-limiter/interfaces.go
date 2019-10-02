package rateLimit

import "github.com/Pushwoosh/go-connection-pool/pkg/message"

type RateLimiter interface {
	Serve(in chan message.Message, out chan message.Message) error
}
