module github.com/thesyncim/goav/examples/transactional-writer

go 1.27.4

require (
	github.com/thesyncim/goav v0.1.0-alpha.1
	github.com/thesyncim/goav/goavtest/expect v0.0.0-20261124192816-69fd6735f94b
)

require github.com/google/go-cmp v0.7.0 // indirect

replace github.com/thesyncim/goav => ../..

replace github.com/thesyncim/goav/goavtest/expect => ../../goavtest/expect
