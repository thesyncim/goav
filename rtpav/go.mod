module github.com/thesyncim/goav/rtpav

go 1.26.2

require (
	github.com/pion/rtcp v1.2.16
	github.com/pion/rtp v1.10.2
	github.com/thesyncim/goav v0.0.0
)

require github.com/thesyncim/goav1 v0.0.0-20260605233508-19454059211a

require (
	github.com/pion/randutil v0.1.0 // indirect
	github.com/thesyncim/goh264 v0.0.0-20260605215817-c3ebee4f35c3 // indirect
	github.com/thesyncim/gopus v0.1.1 // indirect
	github.com/thesyncim/govpx v0.0.0-20260606015758-d64581a69a6f // indirect
)

replace github.com/thesyncim/goav => ../
