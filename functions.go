package goav

import (
	"github.com/thesyncim/goav/internal/compile"
	"github.com/thesyncim/goav/pipeline"
)

func validateSinkComponent(sink pipeline.Sink) error {
	if sink == nil {
		return errNilSink
	}
	if validator, ok := sink.(compile.ComponentValidator); ok {
		if err := validator.ValidateComponent(); err != nil {
			return err
		}
	}
	return nil
}
