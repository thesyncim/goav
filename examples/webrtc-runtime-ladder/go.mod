module github.com/thesyncim/goav/examples/webrtc-runtime-ladder

go 1.26

require (
	github.com/pion/interceptor v0.1.45
	github.com/pion/rtcp v1.2.16
	github.com/pion/webrtc/v4 v4.2.14
	github.com/thesyncim/goav v0.0.0
	github.com/thesyncim/goav/rtpav v0.0.0
	github.com/thesyncim/goav/webrtcav v0.0.0
)

require (
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/ncruces/go-strftime v1.0.0 // indirect
	github.com/pion/datachannel v1.6.0 // indirect
	github.com/pion/dtls/v3 v3.1.3 // indirect
	github.com/pion/ice/v4 v4.2.7 // indirect
	github.com/pion/logging v0.2.4 // indirect
	github.com/pion/mdns/v2 v2.1.0 // indirect
	github.com/pion/randutil v0.1.0 // indirect
	github.com/pion/rtp v1.10.2 // indirect
	github.com/pion/sctp v1.10.0 // indirect
	github.com/pion/sdp/v3 v3.0.18 // indirect
	github.com/pion/srtp/v3 v3.0.11 // indirect
	github.com/pion/stun/v3 v3.1.4 // indirect
	github.com/pion/transport/v4 v4.0.2 // indirect
	github.com/pion/turn/v5 v5.0.7 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	github.com/thesyncim/goaac v0.0.0-20260616210027-501df73f9728 // indirect
	github.com/thesyncim/goav1 v0.0.0-20260707172135-a28b7c8d41f1 // indirect
	github.com/thesyncim/goh264 v0.0.0-20260630205627-5ed6a4334be7 // indirect
	github.com/thesyncim/gopus v0.1.1 // indirect
	github.com/thesyncim/govpx v0.0.0-20260713173815-a8f2d0665f2e // indirect
	github.com/wlynxg/anet v0.0.5 // indirect
	golang.org/x/crypto v0.48.0 // indirect
	golang.org/x/net v0.50.0 // indirect
	golang.org/x/sys v0.44.0 // indirect
	golang.org/x/time v0.14.0 // indirect
	modernc.org/libc v1.73.1 // indirect
	modernc.org/mathutil v1.7.1 // indirect
	modernc.org/memory v1.11.0 // indirect
)

replace (
	github.com/thesyncim/goav => ../..
	github.com/thesyncim/goav/rtpav => ../../rtpav
	github.com/thesyncim/goav/webrtcav => ../../webrtcav
)
