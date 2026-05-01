// Package clock implements vector clocks for tracking causality in distributed systems.
//
// A VectorClock is a map of nodeID → logical counter. It is always immutable
// from the caller's perspective — every mutating operation returns a NEW clock.
// This makes clocks safe to share across goroutines without locks.
package clock

import (
	"fmt"
	"sort"
	"strings"
)

type Relation int

const (
	// HappensBefore means A caused B — A is an ancestor of B.
	// Safe to discard A.
	HappensBefore Relation = iota

	// HappensAfter means B caused A — B is an ancestor of A.
	// Safe to discard B.
	HappensAfter

	// Concurrent means neither clock dominates — both represent
	// independent writes that diverged from a common ancestor.
	// Both versions MUST be retained as siblings.
	Concurrent

	// Equal means both clocks are identical — exact same version.
	Equal
)

func (r Relation) String() string {
	switch r {
	case HappensBefore:
		return "HappensBefore"
	case HappensAfter:
		return "HappensAfter"
	case Concurrent:
		return "Concurrent"
	case Equal:
		return "Equal"
	default:
		return "Unknown"
	}
}

// VectorClock maps nodeID → logical counter.
// Zero value is a valid, empty clock.
// IMMUTABLE: all operations return new clocks.
type VectorClock map[string]uint64

// New returns an empty VectorClock.
func New() VectorClock {
	return make(VectorClock)
}

// Increment returns a NEW clock with the counter for nodeID incremented by 1.
// This must be called every time nodeID performs a write.
//
//	vc = vc.Increment("nodeA")
func (vc VectorClock) Increment(nodeID string) VectorClock {
	newVc := vc.clone()
	newVc[nodeID]++
	return newVc
}

// Merge returns a new clock that is component-wise maximum of vc and other.
func (vc VectorClock) Merge(other VectorClock) VectorClock {
	merged := vc.clone()
	for nodeId, otherVal := range other {
		if otherVal > merged[nodeId] {
			merged[nodeId] = otherVal
		}
	}

	return merged
}

// Compare returns the causal Relation between vc and other.
// Algorithm:
//
//	Track two booleans: selfAhead, otherAhead
//	Iterate over union of all nodeIDs.
//	If self[n] > other[n] for any n → selfAhead = true
//	If other[n] > self[n] for any n → otherAhead = true
//
// Causality Relation depends on selfAhead and otherAhead
func (vc VectorClock) Compare(other VectorClock) Relation {
	var selfAhead, otherAhead bool

	// Check for all keys in vc
	for nodeID, selfVal := range vc {
		otherVal := other[nodeID] // if nodeID does not exists, implicitly 0 as per map behaviour

		// if for at least one key the counter in strictly greater than in other,
		// we can deterministicly say, that prior clock is ahead of latter
		if selfVal > otherVal {
			selfAhead = true
		}
		if otherVal > selfVal {
			otherAhead = true
		}
	}

	// Check keys in other that may not be in vc
	// Example: vc = {A:1, B:1}, other: {C: 1}
	for nodeID, otherVal := range other {
		if _, exists := vc[nodeID]; !exists {
			// vc[nodeID] is implicitly 0
			if otherVal > 0 {
				otherAhead = true
			}
		}
	}

	switch {
	case !selfAhead && !otherAhead:
		return Equal
	case selfAhead && !otherAhead:
		return HappensAfter
	case !selfAhead && otherAhead:
		return HappensBefore
	default:
		return Concurrent
	}
}

// IsConcurrentWith returns true neither clock is causally ahead
func (vc VectorClock) IsConcurrentWith(other VectorClock) bool {
	return vc.Compare(other) == Concurrent
}

// Dominates return true if vc happens-after or equals other
func (vc VectorClock) Dominates(other VectorClock) bool {
	res := vc.Compare(other)
	return res == HappensAfter || res == Equal
}

// Clone returns a deep copy of the clock.
// Exported so storage/node layers can snapshot clocks safely.
func (vc VectorClock) Clone() VectorClock {
	return vc.clone()
}

// String returns a deterministic human-readable representation.
// Sorted by nodeID so output is stable across runs.
func (vc VectorClock) String() string {
	if len(vc) == 0 {
		return "{}"
	}

	keys := make([]string, 0, len(vc))
	for k := range vc {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	parts := make([]string, 0, len(vc))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s:%d", k, vc[k]))
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

// -- helpers

func (vc VectorClock) clone() VectorClock {
	c := make(VectorClock, len(vc))
	for k, v := range vc {
		c[k] = v
	}
	return c
}
