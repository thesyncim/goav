package compile

import (
	"testing"

	"github.com/thesyncim/goav/av"
)

func TestMetadataEqualContracts(t *testing.T) {
	if !MetadataEqual(nil, nil) {
		t.Fatal("nil metadata should equal nil metadata")
	}
	if MetadataEqual(nil, av.Metadata{}) {
		t.Fatal("nil metadata should not equal an empty non-nil map")
	}
	if !MetadataEqual(av.Metadata{"profile": "main"}, av.Metadata{"profile": "main"}) {
		t.Fatal("equal metadata did not compare equal")
	}
	if MetadataEqual(av.Metadata{"profile": "main"}, av.Metadata{"profile": "high"}) {
		t.Fatal("different values compared equal")
	}
	if MetadataEqual(av.Metadata{"profile": "main"}, av.Metadata{"profile": "main", "level": "3.1"}) {
		t.Fatal("different lengths compared equal")
	}
}
