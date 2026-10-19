package compile

import "testing"

func TestBytesEqualPreservesNilDistinction(t *testing.T) {
	if !BytesEqual(nil, nil) {
		t.Fatal("nil byte slices should compare equal")
	}
	if BytesEqual(nil, []byte{}) {
		t.Fatal("nil and empty byte slices should remain distinct")
	}
	if !BytesEqual([]byte{1, 2, 3}, []byte{1, 2, 3}) {
		t.Fatal("equal byte slices did not compare equal")
	}
	if BytesEqual([]byte{1, 2, 3}, []byte{1, 2, 4}) {
		t.Fatal("different byte slices compared equal")
	}
}
