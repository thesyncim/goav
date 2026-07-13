package goav_test

import (
	"github.com/thesyncim/goav"
	runconfig "github.com/thesyncim/goav/runconfig"
)

func mustRuntime(options ...runconfig.Option) *goav.Runtime {
	runtime, err := goav.New(options...)
	if err != nil {
		panic(err)
	}
	return runtime
}
