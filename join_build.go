package goav

const joinStagePreallocDepthLimit = 64

func joinStagePreallocDepth(rt *Runtime) int {
	if rt == nil || rt.buffer.IsDirect() {
		return 1
	}
	depth := max(rt.buffer.Capacity, 1)
	if depth > joinStagePreallocDepthLimit {
		return joinStagePreallocDepthLimit
	}
	return depth
}
