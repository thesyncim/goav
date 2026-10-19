package compile

import "testing"

func TestWorkDestinationNameAndIDHelperContracts(t *testing.T) {
	destinations := []WorkDestination{
		{ID: "destination/archive", Name: "archive"},
		{ID: "destination/preview", Name: "preview"},
		{ID: "destination/nameless"},
	}
	ids := WorkDestinationIDsByName(destinations)
	if got := ids["archive"]; got != "destination/archive" {
		t.Fatalf("archive id = %q", got)
	}
	if _, ok := ids[""]; ok {
		t.Fatalf("empty destination name should not be indexed: %v", ids)
	}

	if got := WorkDestinationIDForName(ids, "archive"); got != "destination/archive" {
		t.Fatalf("workDestinationIDForName hit = %q", got)
	}
	if got := WorkDestinationIDForName(ids, "new"); got != "destination/new" {
		t.Fatalf("workDestinationIDForName miss = %q", got)
	}
	if got := WorkDestinationIDForName(ids, ""); got != "destination/unnamed" {
		t.Fatalf("workDestinationIDForName empty = %q", got)
	}

	if got := WorkDestinationNameByID(destinations, "destination/preview"); got != "preview" {
		t.Fatalf("workDestinationNameByID hit = %q", got)
	}
	if got := WorkDestinationNameByID(destinations, "destination/missing"); got != "destination/missing" {
		t.Fatalf("workDestinationNameByID miss = %q", got)
	}
}
