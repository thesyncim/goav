package integration

import (
	"github.com/thesyncim/goav"
	runconfig "github.com/thesyncim/goav/runconfig"
)

func mustGoAVRuntime(options ...runconfig.Option) *goav.Runtime {
	runtime, err := goav.New(options...)
	if err != nil {
		panic(err)
	}
	return runtime
}
