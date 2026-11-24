module github.com/thesyncim/goav/rtpav

go 1.27.4

require (
	github.com/pion/rtcp v1.2.19
	github.com/pion/rtp v1.10.5
	github.com/thesyncim/goav v0.1.0-alpha.4
	github.com/thesyncim/goav1 v0.0.0-20261124221443-8f29f0adb550
)

require (
	github.com/pion/randutil v0.1.0 // indirect
	github.com/thesyncim/goh264 v0.0.0-20260714162114-14b36a2bfcc4 // indirect
	github.com/thesyncim/gopus v0.2.8 // indirect
	github.com/thesyncim/govpx v0.2.0 // indirect
)

replace github.com/thesyncim/goav => ../
