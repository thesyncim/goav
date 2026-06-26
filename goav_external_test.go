package goav_test

import (
	"github.com/thesyncim/goav"
	goavruntime "github.com/thesyncim/goav/runtime"
)

func mustRuntime(options ...goavruntime.Option) *goav.Runtime {
	runtime, err := goav.New(options...)
	if err != nil {
		panic(err)
	}
	return runtime
}
