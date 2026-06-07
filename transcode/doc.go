// Package transcode defines contracts for multi-output transcoding plans.
// Plans describe branches, ordered steps, labels, and output selection; runtime
// compilers decide how to share decode work and attach stages, filters,
// encoders, and muxers.
package transcode
