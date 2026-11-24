module github.com/thesyncim/goav/examples/webrtc-runtime-ladder

go 1.27.4

require (
	github.com/pion/interceptor v0.1.49
	github.com/pion/rtcp v1.2.19
	github.com/pion/webrtc/v4 v4.2.24
	github.com/thesyncim/goav v0.1.0-alpha.4
	github.com/thesyncim/goav/rtpav v0.0.0-20261124213030-4062f221251b
	github.com/thesyncim/goav/webrtcav v0.0.0-20261124213030-4062f221251b
)

require (
	github.com/google/uuid v1.6.0 // indirect
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
	github.com/thesyncim/goav1 v0.0.0-20261124211549-c9e69d7fdff6 // indirect
	github.com/thesyncim/goh264 v0.0.0-20260714162114-14b36a2bfcc4 // indirect
	github.com/thesyncim/gopus v0.2.8 // indirect
	github.com/thesyncim/govpx v0.1.1-0.20261124220323-9327953bd8dc // indirect
	github.com/wlynxg/anet v0.0.5 // indirect
	golang.org/x/crypto v0.58.0 // indirect
	golang.org/x/net v0.61.0 // indirect
	golang.org/x/sys v0.49.0 // indirect
	golang.org/x/time v0.16.0 // indirect
)

replace (
	github.com/thesyncim/goav => ../..
	github.com/thesyncim/goav/rtpav => ../../rtpav
	github.com/thesyncim/goav/webrtcav => ../../webrtcav
)
