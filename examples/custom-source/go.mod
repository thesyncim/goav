module github.com/thesyncim/goav/examples/custom-source

go 1.27.4

require (
	github.com/thesyncim/goav v0.0.0-20261124164349-bccbd6b0efad
	github.com/thesyncim/goav/goavtest/expect v0.0.0-20261124164349-bccbd6b0efad
)

require github.com/google/go-cmp v0.7.0 // indirect

replace github.com/thesyncim/goav => ../..

replace github.com/thesyncim/goav/goavtest/expect => ../../goavtest/expect
