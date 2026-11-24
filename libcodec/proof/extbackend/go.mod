module github.com/thesyncim/goav/libcodec/proof/extbackend

go 1.27.4

require (
	github.com/thesyncim/goav v0.1.0-alpha.4
	github.com/thesyncim/goav/libcodec v0.0.0-20261124213030-4062f221251b
)

require (
	github.com/ebitengine/purego v0.11.0 // indirect
	github.com/thesyncim/goav1 v0.0.0-20261124211549-c9e69d7fdff6 // indirect
	github.com/thesyncim/goh264 v0.0.0-20260714162114-14b36a2bfcc4 // indirect
	github.com/thesyncim/gopus v0.2.8 // indirect
	github.com/thesyncim/govpx v0.1.0 // indirect
)

replace (
	github.com/thesyncim/goav => ../../..
	github.com/thesyncim/goav/libcodec => ../..
)
