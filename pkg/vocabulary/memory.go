// Package vocabulary derives the agent's allowed label set from the registered
// fleet — backend-agnostic, computed from what discovery produced. Memory is the
// one ordered dimension: its buckets are CONTIGUOUS GB ranges whose boundaries
// fall at quantiles of the fleet's distinct per-node memory sizes, so each range
// corresponds to real clusters and the ranges cover the whole line (any estimate
// lands in exactly one). Range strings ARE the boundaries — self-documenting, no
// small/medium/large legend needed.
package vocabulary

import (
	"fmt"
)

// StandardMemoryBuckets is the fixed memory vocabulary, absolute rather than
// fleet relative.
var StandardMemoryBuckets = []string{"0-16GB", "16-64GB", "64-192GB", "192GB+"}

// StandardRangeFor returns the standard bucket a node size falls into.
func StandardRangeFor(gb int) string { return RangeFor(gb, StandardMemoryBuckets) }

// RangeFor returns the label (from MemoryRanges output) that a given GB size
// falls into — used to tag each cluster's memory subsystem.
func RangeFor(gb int, labels []string) string {
	for _, l := range labels {
		var lo, hi int
		if _, err := fmt.Sscanf(l, "%d-%dGB", &lo, &hi); err == nil {
			if gb >= lo && gb < hi {
				return l
			}
			continue
		}
		if _, err := fmt.Sscanf(l, "%dGB+", &lo); err == nil {
			if gb >= lo {
				return l
			}
		}
	}
	return ""
}
