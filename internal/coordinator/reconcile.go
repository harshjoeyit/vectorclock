package coordinator

import (
	"encoding/json"
	"sort"
	"time"

	"github.com/harshjoeyit/vectorclock/internal/clock"
	"github.com/harshjoeyit/vectorclock/internal/storage"
)

// ReconcileFunc controls what happens when concurrent siblings are found on read:
//   - UnionMerge: merge and write back automatically
//   - LastWriteWins (LWW): pick winner by wall clock and write back
//   - ReturnAll: return all siblings to caller, let them resolve
type ReconcileFunc func(key string, siblings []storage.VersionedValue) storage.VersionedValue

// LastWriteWins resolves conflicts by choosing the sibling with the latest
// wall-clock timestamp, then merging all clocks.
//
// Tradeoffs:
//   - Simple, always produces a single winner.
//   - Silently drops data from the losing siblings.
//   - Only appropriate when data loss is acceptable (e.g. session tokens, counters).
//   - Wall clock skew can make this non-deterministic across nodes.
func LastWriteWins(key string, siblings []storage.VersionedValue) storage.VersionedValue {
	if len(siblings) == 0 {
		panic("reconcile: siblings must not be empty")
	}

	winner := siblings[0]

	for _, sib := range siblings {
		if sib.Timestamp.After(winner.Timestamp) {
			winner = sib
		}
	}

	merged := winner.Clock
	for _, sib := range siblings {
		merged = merged.Merge(sib.Clock)
	}

	return storage.VersionedValue{
		Value:     winner.Value,
		Clock:     merged,
		NodeID:    winner.NodeID,
		Timestamp: time.Now(), // note: current time
	}
}

// UnionMerge resolves conflicts by merging the values as JSON arrays
// and merging all clocks.
//
// Appropriate for set-like values (e.g. shopping carts, tag lists).
// Assumes each sibling's Value is a JSON array of strings.
// Duplicates are removed. Order is stable (sorted).
func UnionMerge(key string, siblings []storage.VersionedValue) storage.VersionedValue {
	if len(siblings) == 0 {
		panic("reconcile: siblings must not be empty")
	}

	var union []string
	seen := make(map[string]struct{}) // to make sure union has unique values

	for _, sib := range siblings {
		var items []string
		if err := json.Unmarshal(sib.Value, &items); err != nil {
			// If a sibling isn't a JSON array fall back to LWW for that sibling
			continue
		}
		for _, item := range items {
			if _, exists := seen[item]; !exists {
				seen[item] = struct{}{}
				union = append(union, item)
			}
		}
	}

	sort.Strings(union)

	merged := clock.New()
	for _, sib := range siblings {
		merged = merged.Merge(sib.Clock)
	}

	value, _ := json.Marshal(union)
	return storage.VersionedValue{
		Value:     value,
		Clock:     merged,
		NodeID:    "coordinator",
		Timestamp: time.Now(),
	}
}

// ReturnAll is a no-op reconciler — it signals to the coordinator
// that conflict resolution should be deferred to the client.
// The coordinator will return all siblings in the response.
//
// This is the true Dynamo model: "syntactic reconciliation" happens
// automatically (dominated versions pruned), but "semantic reconciliation"
// (merging concurrent versions) is the application's responsibility.
func ReturnAll(key string, siblings []storage.VersionedValue) storage.VersionedValue {
	// Sentinel: returning zero value tells coordinator not to write back.
	return storage.VersionedValue{}
}
