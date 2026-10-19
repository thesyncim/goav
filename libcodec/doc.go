// Package libcodec registers codecs that run in shared libraries and OS
// frameworks, bound at run time through purego, so builds keep
// CGO_ENABLED=0: libcodec.VideoToolbox, AudioToolbox, and OpenH264. The kit
// subpackage is how anyone adds another, from any module.
package libcodec
