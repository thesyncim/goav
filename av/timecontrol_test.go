package av

import (
	"math"
	"testing"
	"time"
)

func TestRateMetadataRoundTrip(t *testing.T) {
	event := &Event{Type: EventRate, Metadata: RateMetadata(1.5)}
	rate, ok := EventRateValue(event)
	if !ok || rate != 1.5 {
		t.Fatalf("EventRateValue = %v, %v; want 1.5, true", rate, ok)
	}

	for name, bad := range map[string]*Event{
		"nil":      nil,
		"empty":    {Type: EventRate},
		"garbage":  {Type: EventRate, Metadata: Metadata{MetadataRate: "fast"}},
		"zero":     {Type: EventRate, Metadata: RateMetadata(0)},
		"negative": {Type: EventRate, Metadata: RateMetadata(-2)},
		"infinite": {Type: EventRate, Metadata: RateMetadata(math.Inf(1))},
		"nan":      {Type: EventRate, Metadata: RateMetadata(math.NaN())},
	} {
		if rate, ok := EventRateValue(bad); ok {
			t.Fatalf("EventRateValue(%s) = %v, true; want rejection", name, rate)
		}
	}
}

func TestSegmentEndMetadataRoundTrip(t *testing.T) {
	event := &Event{Type: EventSegment, Metadata: SegmentEndMetadata(20 * time.Second)}
	end, ok := EventSegmentEnd(event)
	if !ok || end != 20*time.Second {
		t.Fatalf("EventSegmentEnd = %v, %v; want 20s, true", end, ok)
	}

	for name, bad := range map[string]*Event{
		"nil":      nil,
		"empty":    {Type: EventSegment},
		"garbage":  {Type: EventSegment, Metadata: Metadata{MetadataSegmentEnd: "later"}},
		"zero":     {Type: EventSegment, Metadata: SegmentEndMetadata(0)},
		"negative": {Type: EventSegment, Metadata: SegmentEndMetadata(-time.Second)},
	} {
		if end, ok := EventSegmentEnd(bad); ok {
			t.Fatalf("EventSegmentEnd(%s) = %v, true; want rejection", name, end)
		}
	}
}
