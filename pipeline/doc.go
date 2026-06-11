// Package pipeline is the executable graph layer under the goav recipe
// grammar: sources, stages, and sinks wired by routes into an inspectable,
// runnable Graph. It owns the flow-control vocabulary — buffer policies, drop
// policies and counters, backpressure and shutdown sentinels — and the
// node-level capabilities (describe, pause, control) the runtime discovers by
// type assertion.
package pipeline
