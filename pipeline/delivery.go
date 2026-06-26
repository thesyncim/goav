package pipeline

import "context"

// Delivery reports what happened to one emitted message across its matching
// targets — the per-message answer that drop counters only aggregate. A
// dropping buffer policy sheds deliberately and without error, so the error
// from Emit alone cannot tell a producer whether this message was delivered;
// Delivery can.
type Delivery struct {
	// Delivered counts matching targets that accepted the message. Buffered
	// targets accept by queueing; direct targets accept when their handler
	// returns nil. Under DropOldest a target may have evicted an older message
	// to admit this one — that eviction lands in the target's drop counters.
	Delivered int
	// Shed counts targets that deliberately dropped this message: a full queue
	// under a dropping policy, an exhausted byte budget, a paused branch, or a
	// queue torn down mid-route.
	Shed int
}

// DeliveryEmitter is an optional capability of an Emitter: EmitDelivery behaves
// exactly like Emit but additionally reports the message's Delivery. Discover
// it by type assertion, like the other optional runner capabilities:
//
//	if de, ok := emitter.(pipeline.DeliveryEmitter); ok {
//	    delivery, err := de.EmitDelivery(ctx, msg)
//	    ...
//	}
//
// The buffered and direct runners both implement it.
type DeliveryEmitter interface {
	EmitDelivery(ctx context.Context, msg *Message) (Delivery, error)
}
