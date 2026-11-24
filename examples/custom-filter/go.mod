module github.com/thesyncim/goav/examples/custom-filter

go 1.27.4

require (
	github.com/thesyncim/goav v0.0.0-20261124170915-2f4654c5324f
	github.com/thesyncim/goav/goavtest/expect v0.0.0-20261124170915-2f4654c5324f
)

require github.com/google/go-cmp v0.7.0 // indirect

replace github.com/thesyncim/goav => ../..

replace github.com/thesyncim/goav/goavtest/expect => ../../goavtest/expect
