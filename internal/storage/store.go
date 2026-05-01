package storage

import (
	"time"

	"github.com/harshjoeyit/vectorclock/internal/clock"
)

type VersionedValue struct {
	Value     []byte // using byte for Value as it is generic for string, blob, etc
	Clock     clock.VectorClock
	Timestamp time.Time // wall clock - only for LWW fallback, NOT ordering
}

// What the store holds per key. If multiple, it's conflict
type KeyEntry struct {
	Siblings []VersionedValue // len > 1 means unreconciled conflict
}
