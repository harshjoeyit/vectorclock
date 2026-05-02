package storage

import (
	"testing"
	"time"

	"github.com/harshjoeyit/vectorclock/internal/clock"
)

// helper: build a VersionedValue quickly
func makeVersion(nodeID string, value string, vc clock.VectorClock) VersionedValue {
	return VersionedValue{
		Value:     []byte(value),
		Clock:     vc,
		NodeID:    nodeID,
		Timestamp: time.Now(),
	}
}

// TestPutFirstWrite verifies a fresh key is always accepted.
func TestPutFirstWrite(t *testing.T) {
	s := New("A") // new store for Node: A
	v := makeVersion("A", `["shoes"]`, clock.VectorClock{"A": 1})

	result := s.Put("cart", v)
	if result != PutAccepted {
		t.Fatalf("first write should be Accepted, got %s", result)
	}

	got := s.Get("cart")
	if !got.Found || len(got.Siblings) != 1 {
		t.Fatalf("expected 1 sibling, got %d", len(got.Siblings))
	}
}

// TestPutLinearUpdate verifies that a causally newer write replaces the old one.
// No conflict — clean lineage.
func TestPutLinearUpdate(t *testing.T) {
	s := New("A")

	v1 := makeVersion("A", `["shoes"]`, clock.VectorClock{"A": 1})
	v2 := makeVersion("B", `["shoes","shirt"]`, clock.VectorClock{"A": 1, "B": 1})

	s.Put("cart", v1)
	result := s.Put("cart", v2)

	if result != PutAccepted {
		t.Fatalf("linear update should be Accepted, got %s", result)
	}

	got := s.Get("cart")
	if got.HasConflict {
		t.Fatal("no conflict expected for linear update")
	}
	if string(got.Siblings[0].Value) != `["shoes","shirt"]` {
		t.Fatalf("expected updated value, got %s", got.Siblings[0].Value)
	}
}

// TestPutStale verifies that a dominated version is silently discarded.
// Simulates a delayed replication message arriving after a newer version.
func TestPutStale(t *testing.T) {
	s := New("A")

	// Write the newer version first
	newer := makeVersion("A", `["shoes","shirt"]`, clock.VectorClock{"A": 2, "B": 1})
	s.Put("cart", newer)

	// Now a stale replicated version arrives
	stale := makeVersion("A", `["shoes"]`, clock.VectorClock{"A": 1})
	result := s.Put("cart", stale)

	if result != PutStale {
		t.Fatalf("stale version should be discarded, got %s", result)
	}

	// Store must still have only the newer version
	got := s.Get("cart")
	if len(got.Siblings) != 1 {
		t.Fatalf("stale write should not create siblings, got %d", len(got.Siblings))
	}
	if string(got.Siblings[0].Value) != `["shoes","shirt"]` {
		t.Fatal("stale write corrupted the store")
	}
}

// TestPutConflict simulates a network partition producing concurrent siblings.
// This is THE core scenario — two nodes write from a common ancestor without
// seeing each other's writes.
func TestPutConflict(t *testing.T) {
	s := New("coordinator")

	// Common ancestor: initial write on node A
	base := makeVersion("A", `["shoes"]`, clock.VectorClock{"A": 1})
	s.Put("cart", base)

	// Partition: node A writes independently → {A:2}
	fromA := makeVersion("A", `["shoes","watch"]`, clock.VectorClock{"A": 2})

	// Partition: node B writes independently → {A:1, B:1}
	fromB := makeVersion("B", `["shoes","shirt"]`, clock.VectorClock{"A": 1, "B": 1})

	// First concurrent write replaces base (it dominates base)
	r1 := s.Put("cart", fromA)
	if r1 != PutAccepted {
		t.Fatalf("first concurrent write should be Accepted, got %s", r1)
	}

	// Second concurrent write — neither dominates the other → CONFLICT
	r2 := s.Put("cart", fromB)
	if r2 != PutConflict {
		t.Fatalf("second concurrent write should produce Conflict, got %s", r2)
	}

	got := s.Get("cart")
	if !got.HasConflict {
		t.Fatal("expected HasConflict=true")
	}
	if len(got.Siblings) != 2 {
		t.Fatalf("expected 2 siblings, got %d", len(got.Siblings))
	}
}

// TestReconcile verifies that after reconciliation only 1 version remains
// and it dominates all previous siblings.
func TestReconcile(t *testing.T) {
	s := New("A")

	// Create conflict
	fromA := makeVersion("A", `["shoes","watch"]`, clock.VectorClock{"A": 2})
	fromB := makeVersion("B", `["shoes","shirt"]`, clock.VectorClock{"A": 1, "B": 1})
	s.Put("cart", fromA)
	s.Put("cart", fromB)

	// Reconciled version merges the clocks (MAX) and merges the values
	mergedClock := fromA.Clock.Merge(fromB.Clock) // {A:2, B:1}
	resolved := makeVersion("coordinator", `["shoes","watch","shirt"]`, mergedClock)

	s.Reconcile("cart", resolved)

	got := s.Get("cart")
	if got.HasConflict {
		t.Fatal("conflict should be resolved after Reconcile")
	}
	if len(got.Siblings) != 1 {
		t.Fatalf("expected 1 sibling after reconciliation, got %d", len(got.Siblings))
	}

	// The resolved version must dominate both original siblings
	if !got.Siblings[0].Clock.Dominates(fromA.Clock) {
		t.Error("resolved clock must dominate fromA")
	}
	if !got.Siblings[0].Clock.Dominates(fromB.Clock) {
		t.Error("resolved clock must dominate fromB")
	}
}

// TestPutIdempotent verifies replaying the same version doesn't create duplicates.
// Critical for at-least-once replication safety.
func TestPutIdempotent(t *testing.T) {
	s := New("A")
	v := makeVersion("A", `["shoes"]`, clock.VectorClock{"A": 1})

	s.Put("cart", v)
	s.Put("cart", v) // replay
	s.Put("cart", v) // replay again

	got := s.Get("cart")
	// Equal clocks are dominated — replays should be treated as stale
	if len(got.Siblings) != 1 {
		t.Fatalf("replayed writes should not create duplicates, got %d siblings", len(got.Siblings))
	}
}

// TestGetDefensiveCopy ensures the caller cannot mutate store state via Get.
func TestGetDefensiveCopy(t *testing.T) {
	s := New("A")
	v := makeVersion("A", `["shoes"]`, clock.VectorClock{"A": 1})
	s.Put("cart", v)

	result := s.Get("cart")
	// Mutate the returned value
	result.Siblings[0].Value = []byte(`["watch"]`)
	result.Siblings[0].Clock["A"] = 999

	// Store must be unchanged
	again := s.Get("cart")
	if string(again.Siblings[0].Value) != `["shoes"]` {
		t.Error("Get returned a mutable reference — store was corrupted")
	}
	if again.Siblings[0].Clock["A"] != 1 {
		t.Error("Get returned a mutable clock reference — store was corrupted")
	}
}
