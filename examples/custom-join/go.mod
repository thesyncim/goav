module github.com/thesyncim/goav/examples/custom-join

go 1.26.4

require github.com/thesyncim/goav v0.0.0

require (
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/google/go-cmp v0.7.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/ncruces/go-strftime v1.0.0 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	github.com/thesyncim/goaac v0.0.0-20260613202902-c08dbfdfe35f // indirect
	github.com/thesyncim/goav/goavtest/expect v0.0.0
	github.com/thesyncim/goav1 v0.0.0-20260614193402-5e8dce2b9457 // indirect
	github.com/thesyncim/goh264 v0.0.0-20260614153501-4f6a0ad24a0a // indirect
	github.com/thesyncim/gopus v0.1.1 // indirect
	github.com/thesyncim/govpx v0.0.0-20260616154555-44d3f28506ff // indirect
	golang.org/x/sys v0.44.0 // indirect
	modernc.org/libc v1.73.1 // indirect
	modernc.org/mathutil v1.7.1 // indirect
	modernc.org/memory v1.11.0 // indirect
)

replace github.com/thesyncim/goav => ../..

replace github.com/thesyncim/goav/goavtest/expect => ../../goavtest/expect
