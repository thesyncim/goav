module github.com/thesyncim/goav/examples/dynamic-audio-room

go 1.26

require github.com/thesyncim/goav v0.0.0

require github.com/thesyncim/goav/goavtest/expect v0.0.0

require github.com/google/go-cmp v0.7.0 // indirect

replace github.com/thesyncim/goav => ../..

replace github.com/thesyncim/goav/goavtest/expect => ../../goavtest/expect
