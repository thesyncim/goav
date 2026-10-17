module github.com/thesyncim/goav/webrtcav

go 1.27.0

toolchain go1.27.2

require (
	github.com/pion/interceptor v0.1.49
	github.com/pion/rtcp v1.2.19
	github.com/pion/rtp v1.10.5
	github.com/pion/webrtc/v4 v4.2.24
	github.com/thesyncim/goav v0.0.0
	github.com/thesyncim/goav/rtpav v0.0.0
)

require (
	github.com/dustin/go-humanize v1.1.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/mattn/go-isatty v0.0.24 // indirect
	github.com/ncruces/go-strftime v1.1.0 // indirect
	github.com/pion/datachannel v1.6.3 // indirect
	github.com/pion/dtls/v3 v3.1.14 // indirect
	github.com/pion/ice/v4 v4.4.7 // indirect
	github.com/pion/logging v0.2.4 // indirect
	github.com/pion/mdns/v2 v2.2.2 // indirect
	github.com/pion/randutil v0.1.0 // indirect
	github.com/pion/sctp v1.12.0 // indirect
	github.com/pion/sdp/v3 v3.0.20 // indirect
	github.com/pion/srtp/v3 v3.1.3 // indirect
	github.com/pion/stun/v4 v4.0.1 // indirect
	github.com/pion/transport/v5 v5.1.1 // indirect
	github.com/pion/turn/v5 v5.1.2 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	github.com/thesyncim/goaac v0.0.0-20260616210027-501df73f9728 // indirect
	github.com/thesyncim/goav1 v0.0.0-20261017161534-cf11a8d2fa8c // indirect
	github.com/thesyncim/goh264 v0.0.0-20260714162114-14b36a2bfcc4 // indirect
	github.com/thesyncim/gopus v0.2.3-0.20261017175526-c210548e8a92 // indirect
	github.com/thesyncim/govpx v0.0.0-20261017170224-928b4e082477 // indirect
	github.com/wlynxg/anet v0.0.5 // indirect
	golang.org/x/crypto v0.58.0 // indirect
	golang.org/x/net v0.61.0 // indirect
	golang.org/x/sys v0.49.0 // indirect
	golang.org/x/time v0.16.0 // indirect
	modernc.org/libc v1.79.0 // indirect
	modernc.org/mathutil v1.7.1 // indirect
	modernc.org/memory v1.12.1 // indirect
)

replace (
	github.com/thesyncim/goav => ../
	github.com/thesyncim/goav/rtpav => ../rtpav
)
