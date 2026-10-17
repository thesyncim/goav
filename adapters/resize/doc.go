// Package resize provides a pure-Go planar 4:2:0 video resize filter adapter.
//
// Each plane is resampled separably: an axis that shrinks averages area (each
// output sample is the coverage-weighted mean of the source span it covers),
// so downscaled renditions do not alias, and an axis that grows interpolates
// linearly. Coefficients are fixed point, built once per geometry, and a
// plane that keeps its size is copied.
package resize
