package compile

import (
	"testing"

	"github.com/thesyncim/goav/av"
)

func TestMetadataEqualContracts(t *testing.T) {
	if !metadataEqual(nil, nil) {
		t.Fatal("nil metadata should equal nil metadata")
	}
	if metadataEqual(nil, av.Metadata{}) {
		t.Fatal("nil metadata should not equal an empty non-nil map")
	}
	if !metadataEqual(av.Metadata{"profile": "main"}, av.Metadata{"profile": "main"}) {
		t.Fatal("equal metadata did not compare equal")
	}
	if metadataEqual(av.Metadata{"profile": "main"}, av.Metadata{"profile": "high"}) {
		t.Fatal("different values compared equal")
	}
	if metadataEqual(av.Metadata{"profile": "main"}, av.Metadata{"profile": "main", "level": "3.1"}) {
		t.Fatal("different lengths compared equal")
	}
}

func TestBytesEqualPreservesNilDistinction(t *testing.T) {
	if !bytesEqual(nil, nil) {
		t.Fatal("nil byte slices should compare equal")
	}
	if bytesEqual(nil, []byte{}) {
		t.Fatal("nil and empty byte slices should remain distinct")
	}
	if !bytesEqual([]byte{1, 2, 3}, []byte{1, 2, 3}) {
		t.Fatal("equal byte slices did not compare equal")
	}
	if bytesEqual([]byte{1, 2, 3}, []byte{1, 2, 4}) {
		t.Fatal("different byte slices compared equal")
	}
}
