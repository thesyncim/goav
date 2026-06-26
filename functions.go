package goav

import "github.com/thesyncim/goav/pipeline"

type componentValidator interface {
	ValidateComponent() error
}

func validateStageComponent(stage pipeline.Stage) error {
	if stage == nil {
		return errNilStage
	}
	if validator, ok := stage.(componentValidator); ok {
		if err := validator.ValidateComponent(); err != nil {
			return err
		}
	}
	return nil
}

func validateSinkComponent(sink pipeline.Sink) error {
	if sink == nil {
		return errNilSink
	}
	if validator, ok := sink.(componentValidator); ok {
		if err := validator.ValidateComponent(); err != nil {
			return err
		}
	}
	return nil
}
