// Package webrtcav moves media between goav and WebRTC peers through Pion.
// In: a Session accepts remote tracks, TrackReaders expose each track as an
// RTP packet feed that survives renegotiation, and webrtcav.Track adapts one
// into a goav source provider for goav.From(...). Out: LocalTrack sends a
// stream on a local track. WHEP and WHIP are the whep and whip URL schemes,
// clients for goav.FromURI and goav.ToURI, registered with runconfig.WithScheme.
package webrtcav
