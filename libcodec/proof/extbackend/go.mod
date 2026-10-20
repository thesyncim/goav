module github.com/thesyncim/goav/libcodec/proof/extbackend

go 1.27.0

toolchain go1.27.2

require (
	github.com/thesyncim/goav v0.0.0-20261020173842-396e291e60d7
	github.com/thesyncim/goav/libcodec v0.0.0-20261020173842-396e291e60d7
)

require (
	github.com/ebitengine/purego v0.11.0 // indirect
	github.com/thesyncim/goav1 v0.0.0-20261017161534-cf11a8d2fa8c // indirect
	github.com/thesyncim/goh264 v0.0.0-20260714162114-14b36a2bfcc4 // indirect
	github.com/thesyncim/gopus v0.2.3-0.20261017175526-c210548e8a92 // indirect
	github.com/thesyncim/govpx v0.0.0-20261017170224-928b4e082477 // indirect
)

replace (
	github.com/thesyncim/goav => ../../..
	github.com/thesyncim/goav/libcodec => ../..
)
