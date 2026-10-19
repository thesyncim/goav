package compile

import (
	"github.com/thesyncim/goav/pipeline"
)

type ComponentValidator interface {
	ValidateComponent() error
}

func ValidateStageComponent(stage pipeline.Stage) error {
	if stage == nil {
		return ErrNilStage
	}
	if validator, ok := stage.(ComponentValidator); ok {
		if err := validator.ValidateComponent(); err != nil {
			return err
		}
	}
	return nil
}
