package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"slices"
	"testing"
	"time"

	"github.com/harshjoeyit/vectorclock/internal/clock"
	"github.com/harshjoeyit/vectorclock/internal/node"
	"github.com/harshjoeyit/vectorclock/internal/storage"
)

// makeCluster creates N in-process nodes (no HTTP) wired to a coordinator.
// Using the store directly avoids network overhead in coordinator tests —
// as we're testing coordinator logic, not HTTP transport (that's node_test.go).
func makeCluster(t *testing.T, n, basePort int, reconcileFn ReconcileFunc) (*Coordinator, []*node.Node) {
	t.Helper()
	nodes := make([]*node.Node, n)
	for i := range nodes {
		addr := fmt.Sprintf(":%d", basePort+i)
		cfg := node.DefaultConfig(addr)
		cfg.MinLatency = 0
		cfg.MaxLatency = 0
		nodes[i] = node.New(fmt.Sprintf("node%d", i+1), cfg)
	}
	q := DefaultQuorum(n)
	coord := New("coordinator", nodes, q, reconcileFn)
	return coord, nodes
}

// injectConflict seeds a conflict directly into node stores,
// bypassing the coordinator. Simulates what happens during a partition.
func injectConflict(t *testing.T, nodes []*node.Node, key string,
	v1val string, v1clock clock.VectorClock,
	v2val string, v2clock clock.VectorClock,
) {
	t.Helper()
	v1 := storage.VersionedValue{
		Value:     []byte(v1val),
		Clock:     v1clock,
		NodeID:    "node1",
		Timestamp: time.Now(),
	}
	v2 := storage.VersionedValue{
		Value:     []byte(v2val),
		Clock:     v2clock,
		NodeID:    "node2",
		Timestamp: time.Now().Add(time.Millisecond),
	}

	// node1 and node2 each get one side of the conflict
	nodes[0].Store().Put(key, v1)
	nodes[1].Store().Put(key, v2)

	// node3 (if present) gets both — simulates having received both via replication
	if len(nodes) > 2 {
		nodes[2].Store().Put(key, v1)
		nodes[2].Store().Put(key, v2)
	}
}

// makeVersion builds a VersionedValue quickly
func makeVersion(nodeID string, value string, vc clock.VectorClock, ts time.Time) storage.VersionedValue {
	return storage.VersionedValue{
		Value:     []byte(value),
		Clock:     vc,
		NodeID:    nodeID,
		Timestamp: ts,
	}
}

// ── Scenario 1: Quorum write succeeds and is readable ────────────────────

func TestQuorumWriteAndRead(t *testing.T) {
	coord, _ := makeCluster(t, 3, 19100, ReturnAll)
	ctx := context.Background()

	_, err := coord.Put(ctx, "cart", []byte(`["shoes"]`))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	result, err := coord.Get(ctx, "cart")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if result.HasConflict {
		t.Fatal("no conflict expected for clean write")
	}
	log.Printf("result: %+v", result)
	if string(result.Siblings[0].Value) != `["shoes"]` {
		t.Fatalf("unexpected value: %s", result.Siblings[0].Value)
	}
}

// ── Scenario 2: Read repair with UnionMerge ──────────────────────────────

// Two concurrent versions are injected directly into node stores.
// On read, coordinator detects conflict, calls UnionMerge, writes back.
// Next read should see a single reconciled version.
func TestReadRepairUnionMerge(t *testing.T) {
	coord, nodes := makeCluster(t, 3, 19100, UnionMerge)
	ctx := context.Background()

	// Inject concurrent siblings — simulates a healed partition
	injectConflict(t, nodes, "cart",
		`["shoes","watch"]`, clock.VectorClock{"node1": 2},
		`["shoes","shirt"]`, clock.VectorClock{"node1": 1, "node2": 1},
	)

	// First read — should detect conflict and repair
	result, err := coord.Get(ctx, "cart")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !result.ReadRepaired {
		t.Fatal("expected read repair to trigger")
	}
	if result.HasConflict {
		t.Fatal("conflict should be resolved after read repair")
	}

	// Value must be the union of both carts
	var items []string
	if err := jsonUnmarshal(result.Siblings[0].Value, &items); err != nil {
		t.Fatalf("bad value: %v", err)
	}
	itemSet := toSet(items)
	for _, expected := range []string{"shoes", "watch", "shirt"} {
		if !itemSet[expected] {
			t.Errorf("union merge missing item: %s, got: %v", expected, items)
		}
	}

	// Give write-back goroutines time to land
	time.Sleep(50 * time.Millisecond)

	// Second read — conflict must be gone on all nodes
	result2, err := coord.Get(ctx, "cart")
	if err != nil {
		t.Fatalf("second Get failed: %v", err)
	}
	if result2.HasConflict {
		t.Fatal("conflict should be gone after read repair propagated")
	}
}

// ── Scenario 3: Read repair with LastWriteWins ───────────────────────────

func TestReadRepairLastWriteWins(t *testing.T) {
	coord, nodes := makeCluster(t, 3, 19100, LastWriteWins)
	ctx := context.Background()

	injectConflict(t, nodes, "session",
		`{"token":"abc"}`, clock.VectorClock{"node1": 1},
		`{"token":"xyz"}`, clock.VectorClock{"node2": 1},
	)

	result, err := coord.Get(ctx, "session")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !result.ReadRepaired {
		t.Fatal("expected read repair")
	}
	// LWW picks the newer wall clock — node2's version has timestamp+1ms
	if string(result.Siblings[0].Value) != `{"token":"xyz"}` {
		t.Fatalf("LWW should pick latest timestamp, got: %s", result.Siblings[0].Value)
	}
}

// ── Scenario 4: ReturnAll defers to client ───────────────────────────────

func TestReturnAllNoAutoRepair(t *testing.T) {
	coord, nodes := makeCluster(t, 3, 19100, ReturnAll)
	ctx := context.Background()

	injectConflict(t, nodes, "cart",
		`["shoes"]`, clock.VectorClock{"node1": 1},
		`["shirt"]`, clock.VectorClock{"node2": 1},
	)

	result, err := coord.Get(ctx, "cart")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if result.ReadRepaired {
		t.Fatal("ReturnAll should NOT auto-repair")
	}
	if !result.HasConflict {
		t.Fatal("conflict should be visible to caller")
	}
	if len(result.Siblings) != 2 {
		t.Fatalf("expected 2 siblings, got %d", len(result.Siblings))
	}
}

// ── Scenario 5: Reconciled clock must dominate all siblings ──────────────

// Verifies the invariant that after read repair, the written-back version's
// clock dominates every sibling that caused the conflict.
func TestReconciledClockDominatesSiblings(t *testing.T) {
	coord, nodes := makeCluster(t, 3, 19100, UnionMerge)
	ctx := context.Background()

	v1clock := clock.VectorClock{"node1": 3, "node2": 1}
	v2clock := clock.VectorClock{"node1": 2, "node2": 2}

	injectConflict(t, nodes, "cart",
		`["shoes","watch","bag"]`, v1clock,
		`["shoes","shirt","hat"]`, v2clock,
	)

	result, _ := coord.Get(ctx, "cart")
	if !result.ReadRepaired {
		t.Fatal("expected read repair")
	}

	reconciledClock := result.Siblings[0].Clock
	if !reconciledClock.Dominates(v1clock) {
		t.Errorf("reconciled clock %s does not dominate v1 %s", reconciledClock, v1clock)
	}
	if !reconciledClock.Dominates(v2clock) {
		t.Errorf("reconciled clock %s does not dominate v2 %s", reconciledClock, v2clock)
	}
}

// ── Scenario 6: Sequential writes never conflict ─────────────────────────

func TestSequentialWritesNoConflict(t *testing.T) {
	coord, _ := makeCluster(t, 3, 19100, UnionMerge)
	ctx := context.Background()

	steps := []string{`["shoes"]`, `["shoes","shirt"]`, `["shoes","shirt","watch"]`}
	for _, v := range steps {
		if _, err := coord.Put(ctx, "cart", []byte(v)); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	result, err := coord.Get(ctx, "cart")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if result.HasConflict {
		t.Fatalf("sequential writes must not conflict:\n siblings: %v", result.Siblings)
	}
	if string(result.Siblings[0].Value) != `["shoes","shirt","watch"]` {
		t.Fatalf("unexpected final value: %s", result.Siblings[0].Value)
	}
}

func TestPruneDominatedOneDominatingOther(t *testing.T) {
	ts1 := time.Now()
	ts2 := time.Now()

	tests := []struct {
		name     string
		input    []storage.VersionedValue
		expected []storage.VersionedValue
	}{
		// EQUAL
		{
			name: "one dominates other",
			input: []storage.VersionedValue{
				makeVersion("A", `["shoes"]`, clock.VectorClock{"A": 1}, ts2),
				makeVersion("A", `["shoes","shirt"]`, clock.VectorClock{"A": 1, "B": 1}, ts1),
			},
			expected: []storage.VersionedValue{
				makeVersion("A", `["shoes","shirt"]`, clock.VectorClock{"A": 1, "B": 1}, ts1),
			},
		},
		{
			name: "one dominates all",
			input: []storage.VersionedValue{
				makeVersion("A", `["shoes"]`, clock.VectorClock{"A": 1}, ts2),
				makeVersion("A", `["shoes","shirt"]`, clock.VectorClock{"A": 1, "B": 1}, ts2),
				makeVersion("A", `["shoes","shirt","pants"]`, clock.VectorClock{"A": 1, "B": 2}, ts1),
			},
			expected: []storage.VersionedValue{
				makeVersion("A", `["shoes","shirt","pants"]`, clock.VectorClock{"A": 1, "B": 2}, ts1),
			},
		},
		{
			name: "none dominating",
			input: []storage.VersionedValue{
				makeVersion("A", `["shoes"]`, clock.VectorClock{"A": 3}, ts2),
				makeVersion("A", `["shoes","shirt"]`, clock.VectorClock{"C": 1, "B": 1}, ts2),
				makeVersion("A", `["shoes","shirt","pants"]`, clock.VectorClock{"A": 1, "B": 2}, ts1),
			},
			expected: []storage.VersionedValue{
				makeVersion("A", `["shoes"]`, clock.VectorClock{"A": 3}, ts2),
				makeVersion("A", `["shoes","shirt"]`, clock.VectorClock{"C": 1, "B": 1}, ts2),
				makeVersion("A", `["shoes","shirt","pants"]`, clock.VectorClock{"A": 1, "B": 2}, ts1),
			},
		},
		{
			name: "all versions are same",
			input: []storage.VersionedValue{
				makeVersion("A", `["shoes"]`, clock.VectorClock{"A": 1}, ts2),
				makeVersion("A", `["shoes"]`, clock.VectorClock{"A": 1}, ts2),
				makeVersion("A", `["shoes"]`, clock.VectorClock{"A": 1}, ts2),
			},
			expected: []storage.VersionedValue{
				makeVersion("A", `["shoes"]`, clock.VectorClock{"A": 1}, ts2),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := pruneDominated(tt.input)
			if !slices.EqualFunc(got, tt.expected, storage.EqualVersionedValue) {
				t.Errorf("got: %s, expected: %s", got, tt.expected)
			}
		})
	}
}

// ── helpers ───────────────────────────────────────────────────────────────

func jsonUnmarshal(data []byte, v any) error {
	return json.Unmarshal(data, v)
}

func toSet(items []string) map[string]bool {
	s := make(map[string]bool, len(items))
	for _, i := range items {
		s[i] = true
	}
	return s
}
