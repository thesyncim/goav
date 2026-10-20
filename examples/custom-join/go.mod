module github.com/thesyncim/goav/examples/custom-join

go 1.27.0

toolchain go1.27.2

require (
	github.com/thesyncim/goav v0.0.0-20261020173842-396e291e60d7
	github.com/thesyncim/goav/goavtest/expect v0.0.0-20261020173842-396e291e60d7
)

require github.com/google/go-cmp v0.7.0 // indirect

replace github.com/thesyncim/goav => ../..

replace github.com/thesyncim/goav/goavtest/expect => ../../goavtest/expect
