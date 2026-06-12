package goav

import (
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/shape"
)

func TestTapNameAndDomainHelperContracts(t *testing.T) {
	if got := defaultDecodedTapName(""); got != "decoded" {
		t.Fatalf("defaultDecodedTapName(empty) = %q, want decoded", got)
	}
	if got := defaultDecodedTapName(av.MediaAudio); got != "audio.decoded" {
		t.Fatalf("defaultDecodedTapName(audio) = %q", got)
	}
	if got := defaultPacketTapName(av.MediaVideo, 99); got != "video.packets" {
		t.Fatalf("defaultPacketTapName(video) = %q", got)
	}
	if got := defaultPacketTapName("", 0); got != "packets" {
		t.Fatalf("defaultPacketTapName(empty,0) = %q", got)
	}
	if got := defaultPacketTapName("", 2); got != "packets-2" {
		t.Fatalf("defaultPacketTapName(empty,2) = %q", got)
	}

	inferred := tapWithDomain(Tap("mid"), shape.DomainFrame)
	if inferred.Name() != "mid" || inferred.Domain() != shape.DomainFrame {
		t.Fatalf("inferred tap = %s/%s", inferred.Name(), inferred.Domain())
	}
	explicit := tapWithDomain(PacketTap("encoded"), shape.DomainFrame)
	if explicit.Name() != "encoded" || explicit.Domain() != shape.DomainPacket {
		t.Fatalf("explicit tap = %s/%s, want packet domain preserved", explicit.Name(), explicit.Domain())
	}
}

func TestWorkDestinationNameAndIDHelperContracts(t *testing.T) {
	destinations := []workDestination{
		{ID: "destination/archive", Name: "archive"},
		{ID: "destination/preview", Name: "preview"},
		{ID: "destination/nameless"},
	}
	ids := workDestinationIDsByName(destinations)
	if got := ids["archive"]; got != "destination/archive" {
		t.Fatalf("archive id = %q", got)
	}
	if _, ok := ids[""]; ok {
		t.Fatalf("empty destination name should not be indexed: %v", ids)
	}

	if got := workDestinationIDForName(ids, "archive"); got != "destination/archive" {
		t.Fatalf("workDestinationIDForName hit = %q", got)
	}
	if got := workDestinationIDForName(ids, "new"); got != "destination/new" {
		t.Fatalf("workDestinationIDForName miss = %q", got)
	}
	if got := workDestinationIDForName(ids, ""); got != "destination/unnamed" {
		t.Fatalf("workDestinationIDForName empty = %q", got)
	}

	if got := workDestinationNameByID(destinations, "destination/preview"); got != "preview" {
		t.Fatalf("workDestinationNameByID hit = %q", got)
	}
	if got := workDestinationNameByID(destinations, "destination/missing"); got != "destination/missing" {
		t.Fatalf("workDestinationNameByID miss = %q", got)
	}
}
