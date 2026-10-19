package pipeline

import "sync/atomic"

// linked is a record that can sit in an mpscQueue: a pointer type with its
// own link field.
type linked[T any] interface {
	*T
	link() *atomic.Pointer[T]
}

// mpscQueue is Dmitry Vyukov's intrusive multi-producer single-consumer
// FIFO. Producers push with one atomic swap; one consumer at a time pops,
// and whatever hands the consumer role on (the direct gate's atomics)
// orders its plain head field from one consumer to the next. It allocates
// nothing: the records are the links. A popped record is out of the queue
// and may be pushed again.
type mpscQueue[T any, P linked[T]] struct {
	tail atomic.Pointer[T]
	head *T
	stub T
}

// init readies the queue. It must not be copied afterwards: the stub is
// referenced by address.
func (q *mpscQueue[T, P]) init() {
	q.head = &q.stub
	q.tail.Store(&q.stub)
}

func (q *mpscQueue[T, P]) push(n *T) {
	P(n).link().Store(nil)
	prev := q.tail.Swap(n)
	P(prev).link().Store(n)
}

// pop returns the oldest record, or nil when there is none or the oldest is
// between its swap and its link.
func (q *mpscQueue[T, P]) pop() *T {
	head := q.head
	next := P(head).link().Load()
	if head == &q.stub {
		if next == nil {
			return nil
		}
		q.head = next
		head = next
		next = P(next).link().Load()
	}
	if next != nil {
		q.head = next
		return head
	}
	if head != q.tail.Load() {
		return nil
	}
	q.push(&q.stub)
	if next = P(head).link().Load(); next != nil {
		q.head = next
		return head
	}
	return nil
}
