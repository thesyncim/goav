package compile

// compositeLayout places one input's top-left corner on the output canvas. One
// layout entry pairs with each input arm, so the compositor knows where to copy
// that arm's frame.
type CompositeLayout struct {
	X, Y int
}
