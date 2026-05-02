package clock

import (
	"testing"
)

// TestCompare is the most critical test — covers all 4 causal relationships
// with concrete scenarios that mirror real distributed system events.
func TestCompare(t *testing.T) {
	tests := []struct {
		name     string
		vc       VectorClock
		other    VectorClock
		expected Relation
		// scenario explains the real-world situation being modelled
		scenario string
	}{
		// EQUAL
		{
			name:     "equal/both_empty",
			vc:       VectorClock{},
			other:    VectorClock{},
			expected: Equal,
			scenario: "Two nodes, neither has written anything yet",
		},
		{
			name:     "equal/identical_clocks",
			vc:       VectorClock{"A": 2, "B": 1},
			other:    VectorClock{"A": 2, "B": 1},
			expected: Equal,
			scenario: "Same version replicated to two nodes — exact duplicate",
		},
		{
			name:     "equal/zero_values_omitted",
			vc:       VectorClock{"A": 1},
			other:    VectorClock{"A": 1, "B": 0},
			expected: Equal,
			scenario: "Explicit zero same as missing key — clock is sparse",
		},
		// HAPPENS-BEFORE
		{
			name:     "happens_before/single_node_behind",
			vc:       VectorClock{"A": 1},
			other:    VectorClock{"A": 2},
			expected: HappensBefore,
			scenario: "Node A wrote once (vc), then wrote again (other). vc is ancestor.",
		},
		{
			name:     "happens_before/multi_node_linear",
			vc:       VectorClock{"A": 1, "B": 0},
			other:    VectorClock{"A": 1, "B": 1},
			expected: HappensBefore,
			scenario: "User added shoes on A {A:1}, then added shirt on B {A:1,B:1}. B saw A's write.",
		},
		{
			name:     "happens_before/missing_node_in_self",
			vc:       VectorClock{"A": 1},
			other:    VectorClock{"A": 2, "B": 1},
			expected: HappensBefore,
			scenario: "vc predates other on both dimensions — clear ancestor",
		},
		// HAPPENS-AFTER
		{
			name:     "happens_after/single_node_ahead",
			vc:       VectorClock{"A": 3},
			other:    VectorClock{"A": 1},
			expected: HappensAfter,
			scenario: "vc is a descendant of other — safe to discard other",
		},
		{
			name:     "happens_after/multi_node",
			vc:       VectorClock{"A": 2, "B": 1},
			other:    VectorClock{"A": 1, "B": 1},
			expected: HappensAfter,
			scenario: "Reconciled write (vc) dominates a stale replica (other)",
		},
		// CONCURRENT
		{
			name:     "concurrent/classic_partition",
			vc:       VectorClock{"A": 2, "B": 0},
			other:    VectorClock{"A": 1, "B": 1},
			expected: Concurrent,
			scenario: "Network partition: A wrote {A:2}, B wrote {A:1,B:1}. Neither saw the other's latest write.",
		},
		{
			name:     "concurrent/disjoint_nodes",
			vc:       VectorClock{"A": 1},
			other:    VectorClock{"B": 1},
			expected: Concurrent,
			scenario: "Two nodes wrote independently, never communicated. Classic conflict.",
		},
		{
			name:     "concurrent/multi_node_cross",
			vc:       VectorClock{"A": 3, "B": 1, "C": 0},
			other:    VectorClock{"A": 2, "B": 2, "C": 0},
			expected: Concurrent,
			scenario: "A is ahead on A's counter, B is ahead on B's counter — concurrent divergence",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.vc.Compare(tt.other)
			if got != tt.expected {
				t.Errorf(
					"\nScenario: %s\n  vc=%s other=%s\n  got=%s want=%s",
					tt.scenario, tt.vc, tt.other, got, tt.expected,
				)
			}
		})
	}
}

// TestCompareSymmetry verifies that Compare is anti-symmetric:
// if A happens-before B, then B happens-after A.
// Concurrent must be symmetric: if A||B then B||A.
func TestCompareSymmetry(t *testing.T) {
	pairs := []struct {
		name string
		a, b VectorClock
	}{
		{"linear", VectorClock{"A": 1}, VectorClock{"A": 2}},
		{"concurrent", VectorClock{"A": 2}, VectorClock{"B": 1}},
		{"equal", VectorClock{"A": 1}, VectorClock{"A": 1}},
	}

	for _, p := range pairs {
		t.Run(p.name, func(t *testing.T) {
			ab := p.a.Compare(p.b)
			ba := p.b.Compare(p.a)

			switch ab {
			case HappensBefore:
				if ba != HappensAfter {
					t.Errorf("a<b but b is not >a: ab=%s ba=%s", ab, ba)
				}
			case HappensAfter:
				if ba != HappensBefore {
					t.Errorf("a>b but b is not <a: ab=%s ba=%s", ab, ba)
				}
			case Concurrent:
				if ba != Concurrent {
					t.Errorf("concurrent must be symmetric: ab=%s ba=%s", ab, ba)
				}
			case Equal:
				if ba != Equal {
					t.Errorf("equal must be symmetric: ab=%s ba=%s", ab, ba)
				}
			}
		})
	}
}

// TestIncrement verifies only the writer's counter advances.
func TestIncrement(t *testing.T) {
	vc := VectorClock{"A": 1, "B": 2}
	next := vc.Increment("A")

	if next["A"] != 2 {
		t.Errorf("A should be 2, got %d", next["A"])
	}
	if next["B"] != 2 {
		t.Errorf("B should still be 2, got %d", next["B"])
	}
	// original must be unchanged (immutability)
	if vc["A"] != 1 {
		t.Error("Increment mutated original clock — must be immutable")
	}
}

// TestIncrementNewNode verifies a new node starts at 1.
func TestIncrementNewNode(t *testing.T) {
	vc := New()
	vc = vc.Increment("NodeX")
	if vc["NodeX"] != 1 {
		t.Errorf("new node first write should be 1, got %d", vc["NodeX"])
	}
}

// TestMerge verifies component-wise maximum.
func TestMerge(t *testing.T) {
	tests := []struct {
		name     string
		a, b     VectorClock
		expected VectorClock
	}{
		{
			name:     "basic_merge",
			a:        VectorClock{"A": 2, "B": 1},
			b:        VectorClock{"A": 1, "B": 3},
			expected: VectorClock{"A": 2, "B": 3},
		},
		{
			name:     "merge_with_missing_key",
			a:        VectorClock{"A": 1},
			b:        VectorClock{"A": 1, "B": 2},
			expected: VectorClock{"A": 1, "B": 2},
		},
		{
			name:     "merge_with_missing_key_2",
			a:        VectorClock{"A": 1},
			b:        VectorClock{"B": 2},
			expected: VectorClock{"A": 1, "B": 2},
		},
		{
			name:     "merge_dominates_both_siblings",
			a:        VectorClock{"A": 2, "B": 0}, // concurrent sibling 1
			b:        VectorClock{"A": 1, "B": 1}, // concurrent sibling 2
			expected: VectorClock{"A": 2, "B": 1}, // merged dominates both
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			merged := tt.a.Merge(tt.b)
			for node, exp := range tt.expected {
				if merged[node] != exp {
					t.Errorf("node %s: got %d want %d", node, merged[node], exp)
				}
			}
			// merged must dominate both inputs
			if r := merged.Dominates(tt.a); !r {
				t.Error("merged should dominate a, but does not")
			}
			if r := merged.Dominates(tt.b); !r {
				t.Error("merged should dominate b, but does not")
			}
		})
	}
}

// TestImmutability ensures no operation mutates the receiver.
func TestImmutability(t *testing.T) {
	original := VectorClock{"A": 1, "B": 2}
	snapshot := original.Clone()

	// All these operations must not mutate original
	_ = original.Increment("A")
	_ = original.Merge(VectorClock{"C": 5})

	for k, v := range snapshot {
		if original[k] != v {
			t.Errorf("operation mutated original clock at key %s", k)
		}
	}
}

// TestMergeAfterConflict is a scenario test:
// Simulates the full lifecycle — write, partition, conflict, reconcile.
func TestMergeAfterConflict(t *testing.T) {
	// Step 1: Initial write on node A
	initial := New().Increment("A") // {A:1}

	// Step 2: Replicated to B, then B writes (linear)
	partitionB := initial.Increment("B") // {A:1, B:1}

	// Step 3: Network partition — A writes independently
	partitionA := initial.Increment("A") // {A:2} — A didn't see B's write

	// Verify conflict is detected
	if partitionA.Compare(partitionB) != Concurrent {
		t.Fatal("expected concurrent versions after partition")
	}

	// Step 4: Reconcile — merge the two siblings
	reconciled := partitionA.Merge(partitionB) // {A:2, B:1}

	// Reconciled must dominate both siblings
	if !reconciled.Dominates(partitionA) {
		t.Error("reconciled must dominate partitionA")
	}
	if !reconciled.Dominates(partitionB) {
		t.Error("reconciled must dominate partitionB")
	}

	// After reconciliation, no new conflicts should arise with either sibling
	if reconciled.IsConcurrentWith(partitionA) || reconciled.IsConcurrentWith(partitionB) {
		t.Error("reconciled should not be concurrent with bother partitions")
	}
}
