package compile

import (
	"maps"

	"github.com/thesyncim/goav/av"
)

func CloneMetadata(metadata av.Metadata) av.Metadata {
	if metadata == nil {
		return nil
	}
	cloned := make(av.Metadata, len(metadata))
	maps.Copy(cloned, metadata)
	return cloned
}
