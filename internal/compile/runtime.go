package compile

import (
	"errors"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
)

// Internal build-refusal causes. Public build-shape matching lives on
// BuildError.Family and BuildError.Code; these only keep package-internal
// errors.Is checks and low-level unwrap chains stable.
var (
	// errNilSource reports a nil source handed to a builder or constructor.
	ErrNilSource = errors.New("goav: nil source")
	// errNilReader reports a nil reader handed to FileInput.
	ErrNilReader = errors.New("goav: nil input reader")
	// errNilStage reports a nil stage handed to a builder or .Do(...).
	ErrNilStage = errors.New("goav: nil stage")
	// errNilSink reports a nil sink handed to a builder or goav.Sink(...).
	ErrNilSink = errors.New("goav: nil sink")
	// errNilWriter reports a nil writer handed to a byte destination.
	ErrNilWriter = errors.New("goav: nil writer")
	// errNilDestinationProvider reports a nil provider.Destination handed to Custom.
	ErrNilDestinationProvider = errors.New("goav: nil destination provider")
)

type EncodeRequest struct {
	Name     string
	Selector av.StreamSelector
	Config   codec.EncodeConfig
}

type DecodeRequest struct {
	Selector av.StreamSelector
	Config   codec.Spec
}
