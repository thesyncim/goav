// Package expertgraph is the one hook between the goav root package and the
// expert package. Root installs Open at init; expert calls it. Neither side
// imports the other's types through it, and Runtime's method set stays free
// of the expert seam.
package expertgraph

// Open returns the expert graph core for a *goav.Runtime, or nil when the
// value is not one. The root package sets it during initialization, which
// completes before any package importing root (expert among them) runs.
var Open func(runtime any) any
