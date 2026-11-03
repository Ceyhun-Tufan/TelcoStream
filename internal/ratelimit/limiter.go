package ratelimit

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type Limiter struct {
	client *redis.Client
	script *redis.Script
	limit  int
	window time.Duration
}

func NewLimiter(client *redis.Client, script *redis.Script, limit int, window time.Duration) *Limiter {

	return &Limiter{
		client: client,
		script: script,
		limit:  limit,
		window: window,
	}

}

func (l *Limiter) Allow(ctx context.Context, identifier string) (bool, error) {
	key := fmt.Sprintf("ratelimit:%s", identifier)
	nowMs := time.Now().UnixMilli()
	windowMs := l.window.Milliseconds()
	res, err := l.script.Run(ctx, l.client, []string{key}, nowMs, windowMs, l.limit).Int()
	if err != nil {
		return false, fmt.Errorf("rate limiter redis error: %w", err)
	}
	return res == 1, nil
}
