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
	"sort"
)

// MemoryRanges turns per-node memory sizes (GiB) into up to `buckets` contiguous
// range labels. Boundaries are quantiles over the DISTINCT sizes present. With
// fewer than 2 distinct sizes memory can't discriminate clusters, so it returns
// nil (the dimension is dropped, like gpu-vendor when homogeneous).
func MemoryRanges(nodeGB []int, buckets int) []string {
	// distinct, sorted
	seen := map[int]bool{}
	var distinct []int
	for _, g := range nodeGB {
		if g > 0 && !seen[g] {
			seen[g] = true
			distinct = append(distinct, g)
		}
	}
	sort.Ints(distinct)
	if len(distinct) < 2 {
		return nil // not a discriminating dimension
	}
	if buckets < 2 {
		buckets = 2
	}
	if buckets > len(distinct) {
		buckets = len(distinct) // never more ranges than distinct sizes
	}

	// interior boundaries at quantile positions among the distinct sizes
	var bounds []int
	for i := 1; i < buckets; i++ {
		idx := i * len(distinct) / buckets
		if idx >= len(distinct) {
			idx = len(distinct) - 1
		}
		b := distinct[idx]
		if len(bounds) == 0 || b != bounds[len(bounds)-1] {
			bounds = append(bounds, b)
		}
	}

	// contiguous range labels covering [0, +inf)
	labels := make([]string, 0, len(bounds)+1)
	lo := 0
	for _, b := range bounds {
		labels = append(labels, fmt.Sprintf("%d-%dGB", lo, b))
		lo = b
	}
	labels = append(labels, fmt.Sprintf("%dGB+", lo))
	return labels
}

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
