// Package storage implements per-node versioned key-value storage.
//
// Each key holds a set of VersionedValues called "siblings".
// len(siblings) == 1  → no conflict, normal case
// len(siblings)  > 1  → concurrent versions exist, needs reconciliation
package storage

import (
	"bytes"
	"time"

	"github.com/harshjoeyit/vectorclock/internal/clock"
)

// VersionedValue is the atomic unit of storage.
// The vector clock travels WITH the value — never stored separately.
type VersionedValue struct {
	Value     []byte // using byte for Value as it is generic for string, blob, etc
	Clock     clock.VectorClock
	NodeID    string    // which node created this version
	Timestamp time.Time // wall clock - only for LWW fallback, NOT ordering
}

// IsDominatedBy returns true if other is causally newer than this version.
func (v VersionedValue) IsDominatedBy(other VersionedValue) bool {
	return other.Clock.Dominates(v.Clock)
}

func EqualVersionedValue(a, b VersionedValue) bool {
	return bytes.Equal(a.Value, b.Value) &&
		clock.EqualVectorClock(a.Clock, b.Clock) &&
		a.NodeID == b.NodeID &&
		a.Timestamp.Equal(b.Timestamp)
}
