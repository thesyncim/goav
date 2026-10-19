package compile

import (
	"github.com/thesyncim/goav/internal/builderr"
	"github.com/thesyncim/goav/pipeline"
)

type ComponentValidator interface {
	ValidateComponent() error
}

func ValidateStageComponent(stage pipeline.Stage) error {
	if stage == nil {
		return builderr.ErrNilStage
	}
	if validator, ok := stage.(ComponentValidator); ok {
		if err := validator.ValidateComponent(); err != nil {
			return err
		}
	}
	return nil
}
