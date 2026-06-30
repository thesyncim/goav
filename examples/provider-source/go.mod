module github.com/thesyncim/goav/examples/provider-source

go 1.26

require github.com/thesyncim/goav v0.0.0

require (
	github.com/google/go-cmp v0.7.0 // indirect
	github.com/thesyncim/goav/goavtest/expect v0.0.0
)

replace github.com/thesyncim/goav => ../..

replace github.com/thesyncim/goav/goavtest/expect => ../../goavtest/expect
