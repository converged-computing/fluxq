package vocabulary

import (
	"testing"
)

func TestStandardBucketsAreAbsolute(t *testing.T) {
	// Fixed, fleet-independent buckets: the same GB size always maps to the same
	// bucket, whatever else is registered.
	for _, tc := range []struct {
		gb   int
		want string
	}{{3, "0-16GB"}, {12, "0-16GB"}, {28, "16-64GB"}, {120, "64-192GB"}, {248, "192GB+"}} {
		if got := StandardRangeFor(tc.gb); got != tc.want {
			t.Fatalf("%dGB -> %q, want %q", tc.gb, got, tc.want)
		}
	}
}
