package integration

import (
	"github.com/thesyncim/goav"
	goavruntime "github.com/thesyncim/goav/runtime"
)

func mustGoAVRuntime(options ...goavruntime.Option) *goav.Runtime {
	runtime, err := goav.New(options...)
	if err != nil {
		panic(err)
	}
	return runtime
}
