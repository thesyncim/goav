// Package libcodec registers codecs that run in shared libraries and OS
// frameworks, bound at run time through purego, so builds keep
// CGO_ENABLED=0:
//
//	rt, err := bundle.New(libcodec.VideoToolbox(), libcodec.AudioToolbox(), libcodec.OpenH264())
//
// They join the pure-Go codecs in one registry. A request takes the backend
// it names (codec.Backend("openh264"), or backend=openh264 in a program),
// else the most preferred one that fits: hardware, then shared libraries,
// then pure Go (see runconfig.WithCodecPreference). A library loads when the
// runtime is built, never in init. One that is missing, outside the
// releases its backend mirrors, or turned off registers its backend
// descriptor-only, with a status (not-installed, outside-window, disabled)
// and the fix; selection passes it over, and kit.Probe reports it.
//
// Settings come in three tiers: the common knobs (bitrate, rc, quality, qp,
// speed, threads, tune, gop), each backend's own (openh264.entropy=cabac
// scopes one to a backend), and raw library parameters
// (openh264-params="iMaxQp=40:iMinQp=10"). Explain lists a backend's knobs.
//
// Licenses travel with the backends: Declarations returns each one's
// license class, notice, terms, and published releases as plain values.
// Cisco's patent license for its OpenH264 binaries asks a program to show
// "OpenH264 Video Codec provided by Cisco Systems, Inc." (Explain reports it
// as the encoder's Notice), to reproduce Cisco's terms (License.Terms), and
// to let the end user turn the codec off: GOAV_LIBCODEC_DISABLE=openh264
// does, in any goav program, as it does for any backend (a comma-separated
// list, or all).
//
// The kit subpackage is how anyone adds a backend, from any module.
package libcodec
