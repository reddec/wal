package stream

import (
	"context"
	"math/rand"
	"time"
)

type FinishStrategy interface {
	// Executes at the end of stream. If returns error - retries, else commit
	Done(ctx context.Context, err error) error
}

type delay struct {
	Delay  time.Duration
	Jitter time.Duration
}

func (rs *delay) Done(ctx context.Context, err error) error {
	if err != nil {
		realDelay := rs.Delay + time.Duration(rand.Int63n(int64(rs.Jitter)))
		select {
		case <-time.After(realDelay):
			return err // returns err means retry
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func Delay(interval time.Duration, jitter time.Duration) FinishStrategy {
	return &delay{
		Jitter: jitter,
		Delay:  interval,
	}
}

type ignore struct {
}

func (rs *ignore) Done(ctx context.Context, err error) error { return nil }

func Ignore() FinishStrategy { return &ignore{} }
