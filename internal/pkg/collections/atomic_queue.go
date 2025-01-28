package collections

import (
	"sync/atomic"
)

type AtomicQueue[T comparable] struct {
	// Items to be added to channel
	items chan T
	// counter used to incremented and decremented atomically.
	counter uint64
}

func NewAtomicQueue[T comparable]() *AtomicQueue[T] {
	return &AtomicQueue[T]{
		items:   make(chan T, 1),
		counter: 0,
	}
}

func (q *AtomicQueue[T]) Enqueue(item T) {
	// counter variable atomically incremented
	atomic.AddUint64(&q.counter, 1)
	// put item to channel
	q.items <- item
}

func (q *AtomicQueue[T]) Dequeue() T {
	// read item from channel
	item := <-q.items
	// counter variable decremented atomically.
	atomic.AddUint64(&q.counter, ^uint64(0))
	return item
}

func (q *AtomicQueue[T]) IsEmpty() bool {
	return q.counter == 0
}
