module github.com/thesyncim/goav/examples/gio-webrtc-showcase

go 1.27.0

toolchain go1.27.2

require (
	gioui.org v0.10.3
	github.com/ebitengine/oto/v3 v3.5.1
	github.com/pion/interceptor v0.1.49
	github.com/pion/rtcp v1.2.19
	github.com/pion/webrtc/v4 v4.2.24
	github.com/thesyncim/goav v0.0.0-20261020160912-710e3246b213
	github.com/thesyncim/goav/rtpav v0.0.0-20261020160912-710e3246b213
	github.com/thesyncim/goav/webrtcav v0.0.0-20261020160912-710e3246b213
)

require (
	gioui.org/shader v1.0.9 // indirect
	github.com/ebitengine/purego v0.11.1 // indirect
	github.com/go-text/typesetting v0.3.5 // indirect
	github.com/godbus/dbus/v5 v5.2.4 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/jfreymuth/pulse v0.1.3 // indirect
	github.com/pion/datachannel v1.6.3 // indirect
	github.com/pion/dtls/v3 v3.1.14 // indirect
	github.com/pion/ice/v4 v4.4.7 // indirect
	github.com/pion/logging v0.2.4 // indirect
	github.com/pion/mdns/v2 v2.2.2 // indirect
	github.com/pion/randutil v0.1.0 // indirect
	github.com/pion/rtp v1.10.5 // indirect
	github.com/pion/sctp v1.12.0 // indirect
	github.com/pion/sdp/v3 v3.0.20 // indirect
	github.com/pion/srtp/v3 v3.1.3 // indirect
	github.com/pion/stun/v4 v4.0.1 // indirect
	github.com/pion/transport/v5 v5.1.1 // indirect
	github.com/pion/turn/v5 v5.1.2 // indirect
	github.com/thesyncim/goav1 v0.0.0-20261017161534-cf11a8d2fa8c // indirect
	github.com/thesyncim/goh264 v0.0.0-20260714162114-14b36a2bfcc4 // indirect
	github.com/thesyncim/gopus v0.2.3-0.20261017175526-c210548e8a92 // indirect
	github.com/thesyncim/govpx v0.0.0-20261017170224-928b4e082477 // indirect
	github.com/wlynxg/anet v0.0.5 // indirect
	golang.org/x/crypto v0.58.0 // indirect
	golang.org/x/exp/shiny v0.0.0-20261014143641-f0251eb4aa3e // indirect
	golang.org/x/image v0.47.0 // indirect
	golang.org/x/net v0.61.0 // indirect
	golang.org/x/sys v0.49.0 // indirect
	golang.org/x/text v0.43.0 // indirect
	golang.org/x/time v0.16.0 // indirect
)

replace (
	github.com/thesyncim/goav => ../..
	github.com/thesyncim/goav/rtpav => ../../rtpav
	github.com/thesyncim/goav/webrtcav => ../../webrtcav
)
