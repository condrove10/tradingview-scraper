package tradingview

import "sync"

// mailbox delivers values to one callback, in order, on its own goroutine.
//
// push never blocks: the connection goroutine must not wait for user code.
// When the queue is full the oldest value is dropped, so a slow listener
// loses old updates but always receives the newest one.
type mailbox[T any] struct {
	mu       sync.Mutex
	items    []T
	limit    int
	closed   bool
	overflow bool
	wake     chan struct{}
}

func newMailbox[T any](limit int, fn func(T)) *mailbox[T] {
	if limit < 1 {
		limit = 1
	}
	b := &mailbox[T]{limit: limit, wake: make(chan struct{}, 1)}
	go b.run(fn)
	return b
}

// push queues v. It reports true when this push started dropping values
// (once per overflow episode; the episode ends when the queue drains).
func (b *mailbox[T]) push(v T) (overflowStarted bool) {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return false
	}
	if len(b.items) >= b.limit {
		var zero T
		b.items[0] = zero
		b.items = b.items[1:]
		if !b.overflow {
			b.overflow = true
			overflowStarted = true
		}
	}
	b.items = append(b.items, v)
	b.mu.Unlock()
	b.signal()
	return overflowStarted
}

// close discards queued values and stops the goroutine once the callback
// that may be running returns. It is safe to call from inside the callback.
func (b *mailbox[T]) close() {
	b.mu.Lock()
	b.closed = true
	b.items = nil
	b.mu.Unlock()
	b.signal()
}

func (b *mailbox[T]) signal() {
	select {
	case b.wake <- struct{}{}:
	default:
	}
}

func (b *mailbox[T]) run(fn func(T)) {
	for range b.wake {
		for {
			b.mu.Lock()
			if b.closed {
				b.mu.Unlock()
				return
			}
			if len(b.items) == 0 {
				b.overflow = false
				b.mu.Unlock()
				break
			}
			v := b.items[0]
			var zero T
			b.items[0] = zero
			b.items = b.items[1:]
			b.mu.Unlock()
			fn(v)
		}
	}
}
