package mapqueue

import "sync"

type Subscription struct {
	ch     chan struct{}
	bucket *Notification
}

func (sb *Subscription) Close() {
	sb.bucket.lock.Lock()
	for i, s := range sb.bucket.channels {
		if s == sb.ch {
			sb.bucket.channels = append(sb.bucket.channels[:i], sb.bucket.channels[i+1:]...)
			break
		}
	}
	sb.bucket.lock.Unlock()
	close(sb.ch)
}

// Wait for new events
func (sb *Subscription) Wait() <-chan struct{} { return sb.ch }

type Notification struct {
	channels []chan struct{}
	lock     sync.RWMutex
}

// Create new subscription for events
func (not *Notification) Subscribe() *Subscription {
	not.lock.Lock()
	defer not.lock.Unlock()
	ch := make(chan struct{}, 1)
	not.channels = append(not.channels, ch)
	return &Subscription{
		bucket: not,
		ch:     ch,
	}
}

func (not *Notification) notify() {
	not.lock.RLock()
	defer not.lock.RUnlock()
	for _, ch := range not.channels {
		select {
		case ch <- struct{}{}:
		default:

		}
	}
}
