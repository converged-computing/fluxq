package vocabulary

import (
	"reflect"
	"testing"
)

func TestMemoryRangesQuantileContiguous(t *testing.T) {
	// distinct sizes 64,128,256,512 -> 3 contiguous ranges at quantile bounds
	got := MemoryRanges([]int{64, 64, 128, 256, 512, 512}, 3)
	want := []string{"0-128GB", "128-256GB", "256GB+"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ranges: got %v want %v", got, want)
	}
	// every size lands in exactly one range (continuous cover)
	for _, tc := range []struct {
		gb   int
		want string
	}{{50, "0-128GB"}, {128, "128-256GB"}, {200, "128-256GB"}, {256, "256GB+"}, {2000, "256GB+"}} {
		if r := RangeFor(tc.gb, got); r != tc.want {
			t.Fatalf("RangeFor(%d)=%q want %q", tc.gb, r, tc.want)
		}
	}
}

func TestMemoryDroppedWhenNotDiscriminating(t *testing.T) {
	if r := MemoryRanges([]int{256, 256, 256}, 3); r != nil {
		t.Fatalf("uniform memory should drop the dimension, got %v", r)
	}
	// two distinct sizes -> two ranges even if 3 requested
	got := MemoryRanges([]int{128, 512}, 3)
	if !reflect.DeepEqual(got, []string{"0-512GB", "512GB+"}) {
		t.Fatalf("two-size fleet: got %v", got)
	}
}
