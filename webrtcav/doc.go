// Package webrtcav receives media from WebRTC peers through Pion: a Session
// accepts remote tracks, TrackReaders expose each track as an RTP packet
// feed that survives renegotiation, and webrtcav.Track adapts one into a
// goav source provider for From(goav.Input(...)).
package webrtcav
