package node

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/harshjoeyit/vectorclock/internal/clock"
	"github.com/harshjoeyit/vectorclock/internal/storage"
)

// -- helper functions

// startCluster boots N nodes on sequential ports and wires them as peers.
// Returns nodes and a cleanup function.
func startCluster(t *testing.T, n int, basePort int) ([]*Node, func()) {
	t.Helper()

	nodes := make([]*Node, n)
	for i := range nodes {
		addr := fmt.Sprintf(":%d", basePort+i)
		cfg := DefaultConfig(addr)
		cfg.MinLatency = 0
		cfg.MaxLatency = 5 * time.Millisecond
		nodes[i] = New(fmt.Sprintf("node%d", i+1), cfg)
	}

	// Wire peers: every node knows every other node.
	for i, nd := range nodes {
		for j, peer := range nodes {
			if i != j {
				nd.AddPeer(Peer{
					ID:   peer.id,
					Addr: fmt.Sprintf("http://localhost%s", peer.config.addr),
				})
			}
		}
	}

	for _, nd := range nodes {
		if err := nd.Start(); err != nil {
			t.Fatalf("failed to start node: %v", err)
		}
	}

	cleanup := func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		for _, nd := range nodes {
			nd.Stop(ctx)
		}
	}

	return nodes, cleanup
}

// clientPut sends a PUT to the given node address.
func clientPut(t *testing.T, addr, key string, value []byte, baseClock map[string]uint64) PutResponse {
	t.Helper()

	body, _ := json.Marshal(PutRequest{Value: value, BaseClock: baseClock})
	url := fmt.Sprintf("http://localhost%s/keys/%s", addr, key)

	resp, err := http.DefaultClient.Do(mustRequest(http.MethodPut, url, body))
	if err != nil {
		t.Fatalf("PUT failed: %v", err)
	}
	defer resp.Body.Close()

	var pr PutResponse
	json.NewDecoder(resp.Body).Decode(&pr)

	return pr
}

// clientGet reads from the given node address.
func clientGet(t *testing.T, addr, key string) (GetResponse, int) {
	t.Helper()
	url := fmt.Sprintf("http://localhost%s/keys/%s", addr, key)
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return GetResponse{}, 404
	}
	var gr GetResponse
	json.NewDecoder(resp.Body).Decode(&gr)
	return gr, resp.StatusCode
}

func mustRequest(method, url string, body []byte) *http.Request {
	req, _ := http.NewRequest(method, url, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	return req
}

// ── Scenario 1: Linear writes replicate cleanly ──────────────────────────

// TestLinearReplication: write to node1, read from node2.
// Expects: single version, no conflict, value propagated.
func TestLinearReplication(t *testing.T) {
	nodes, cleanup := startCluster(t, 2, 19100)
	defer cleanup()

	clientPut(t, nodes[0].config.addr, "cart", []byte(`["shoes"]`), nil)

	// Give async replication time to land
	time.Sleep(100 * time.Millisecond)

	result, status := clientGet(t, nodes[1].config.addr, "cart")
	if status == 404 {
		t.Fatal("replication did not reach node2")
	}
	if result.HasConflict {
		t.Fatal("no conflict expected for linear write")
	}
	if string(result.Siblings[0].Value) != `["shoes"]` {
		t.Fatalf("unexpected value: %s", result.Siblings[0].Value)
	}
}

// TestConcurrentWritesConflict: partition both nodes (drop=1.0),
// write different values to each, restore, verify siblings exist.
func TestConcurrentWritesConflict(t *testing.T) {
	nodes, cleanup := startCluster(t, 2, 19100)
	defer cleanup()

	// Simulate partition — neither node replicates to the other
	nodes[0].SetDropRate(1.0)
	nodes[1].SetDropRate(1.0)

	// Concurrent writes during partition
	clientPut(t, nodes[0].config.addr, "cart", []byte(`["shoes","watch"]`), nil)
	clientPut(t, nodes[1].config.addr, "cart", []byte(`["shoes","shirt"]`), nil)

	// Give async replication time to land
	time.Sleep(100 * time.Millisecond)

	// Heal partition
	nodes[0].SetDropRate(0)
	nodes[1].SetDropRate(0)

	// Trigger cross-replication by writing again (or check store directly)
	// Here we check the stores directly for the prototype
	r0 := nodes[0].store.Get("cart")
	r1 := nodes[1].store.Get("cart")

	// Each node has its own version — both are valid for now
	if !r0.Found || !r1.Found {
		t.Fatal("both nodes should have a version")
	}

	// Now manually replicate node1's version to node0 to trigger conflict
	// (in real system this happens via gossip/anti-entropy)
	for _, sib := range r1.Siblings {
		result := nodes[0].store.Put("cart", sib)
		t.Logf("manual replicate to node0: %s", result)
	}

	// node0 should now have a conflict
	final := nodes[0].store.Get("cart")
	if !final.HasConflict {
		t.Logf("node0 store: %s", nodes[0].DebugDump())
		t.Fatal("expected conflict after concurrent writes from partition")
	}
	if len(final.Siblings) != 2 {
		t.Fatalf("expected 2 siblings, got %d", len(final.Siblings))
	}
	t.Logf("confirmed conflict:\n%s", nodes[0].DebugDump())
}

// ── Scenario 3: Causal writes — client echoes clock ──────────────────────

// TestCausalWrites: client reads, gets a clock, uses it in the next write.
// Second write must dominate the first — no conflict.
func TestCausalWrites(t *testing.T) {
	nodes, cleanup := startCluster(t, 2, 19100)
	defer cleanup()

	// First write
	r1 := clientPut(t, nodes[0].config.addr, "cart", []byte(`["shoes"]`), nil)
	t.Logf("write1 clock: %v", r1.Clock)

	// Client echoes back the clock it received — establishes causal dependency
	r2 := clientPut(t, nodes[0].config.addr, "cart", []byte(`["shoes","shirt"]`), r1.Clock)
	t.Logf("write2 clock: %v", r2.Clock)

	// Give async replication time to land
	time.Sleep(100 * time.Millisecond)

	result := nodes[0].store.Get("cart")
	if result.HasConflict {
		t.Fatalf("causal writes should not conflict:\n%s", nodes[0].DebugDump())
	}
	if string(result.Siblings[0].Value) != `["shoes","shirt"]` {
		t.Fatalf("expected updated value, got: %s", result.Siblings[0].Value)
	}
}

// ── Scenario 4: Stale replication is discarded ───────────────────────────

// TestStaleReplicationDropped: write v2 to node, then directly inject a
// stale version (lower clock) into the store — simulates a delayed
// replication message arriving after a newer version is already present.
// The stale version must be silently discarded.
func TestStaleReplicationDropped(t *testing.T) {
	nodes, cleanup := startCluster(t, 2, 19100)
	defer cleanup()

	// Write v1, then v2 causally
	r1 := clientPut(t, nodes[0].config.addr, "cart", []byte(`["shoes"]`), nil)
	_ = clientPut(t, nodes[0].config.addr, "cart", []byte(`["shoes","shirt"]`), r1.Clock)

	// Inject a stale version directly into the store —
	// simulates a replication message for v1 arriving late (out of order delivery).
	importClock := clock.VectorClock{"node1": 1} // v1's clock — dominated by v2
	stale := storage.VersionedValue{
		Value:     []byte(`["shoes"]`),
		Clock:     importClock,
		NodeID:    "node1",
		Timestamp: time.Now(),
	}
	result := nodes[0].store.Put("cart", stale)
	if result != storage.PutStale {
		t.Fatalf("late replication of v1 should be PutStale, got %s", result)
	}

	final := nodes[0].store.Get("cart")
	if final.HasConflict {
		t.Fatalf("stale replay should not create conflict:\n%s", nodes[0].DebugDump())
	}
	if string(final.Siblings[0].Value) != `["shoes","shirt"]` {
		t.Fatalf("stale write overwrote newer value: %s", final.Siblings[0].Value)
	}
}
