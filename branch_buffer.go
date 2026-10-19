package goav

// promotedGraphBufferCapacity is the trunk queue depth of a graph that runs
// buffered only because a branch declared .Buffer(...) or its encoders run in
// parallel: deep enough to keep each branch's worker busy, shallow enough to
// bound the latency the queue adds.
const promotedGraphBufferCapacity = 4
