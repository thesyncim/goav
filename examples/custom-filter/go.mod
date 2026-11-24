module github.com/thesyncim/goav/examples/custom-filter

go 1.27.4

require (
	github.com/thesyncim/goav v0.0.0-20261124184026-94a2a5a8513c
	github.com/thesyncim/goav/goavtest/expect v0.0.0-20261124184026-94a2a5a8513c
)

require github.com/google/go-cmp v0.7.0 // indirect

replace github.com/thesyncim/goav => ../..

replace github.com/thesyncim/goav/goavtest/expect => ../../goavtest/expect
