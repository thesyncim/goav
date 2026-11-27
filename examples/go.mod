module github.com/thesyncim/goav/examples

go 1.27.4

require (
	github.com/thesyncim/goav v0.2.3-0.20261127194215-bce3f85b9498
	github.com/thesyncim/goav/goavtest/expect v0.0.0-20261124213030-4062f221251b
)

require (
	github.com/google/go-cmp v0.7.0 // indirect
	github.com/thesyncim/goav1 v0.2.6 // indirect
	github.com/thesyncim/goh264 v0.0.0-20260714162114-14b36a2bfcc4 // indirect
	github.com/thesyncim/gopus v0.2.11 // indirect
	github.com/thesyncim/govpx v0.5.2 // indirect
)

replace (
	github.com/thesyncim/goav => ..
	github.com/thesyncim/goav/goavtest/expect => ../goavtest/expect
)
