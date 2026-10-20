package pipeline

import "fmt"

// FinalizationError identifies one node's actual Close failure. ID distinguishes
// successive nodes that reused a display name; errors.Is matches Cause.
type FinalizationError struct {
	ID    uint64
	Node  NodeRef
	Cause error
}

func (e *FinalizationError) Error() string {
	return fmt.Sprintf("pipeline: cleanup of %q (#%d): %v", e.Node, e.ID, e.Cause)
}
func (e *FinalizationError) Unwrap() error { return e.Cause }
