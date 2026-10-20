module github.com/thesyncim/goav/rtpav

go 1.27.0

toolchain go1.27.2

require (
	github.com/pion/rtcp v1.2.19
	github.com/pion/rtp v1.10.5
	github.com/thesyncim/goav v0.0.0-20261020160912-710e3246b213
	github.com/thesyncim/goav1 v0.0.0-20261017161534-cf11a8d2fa8c
)

require (
	github.com/pion/randutil v0.1.0 // indirect
	github.com/thesyncim/goh264 v0.0.0-20260714162114-14b36a2bfcc4 // indirect
	github.com/thesyncim/gopus v0.2.3-0.20261017175526-c210548e8a92 // indirect
	github.com/thesyncim/govpx v0.0.0-20261017170224-928b4e082477 // indirect
)

replace github.com/thesyncim/goav => ../
