package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/harshjoeyit/vectorclock/internal/clock"
	"github.com/harshjoeyit/vectorclock/internal/node"
	"github.com/harshjoeyit/vectorclock/internal/storage"
)

// ── Cluster bootstrap ─────────────────────────────────────────────────────

type cluster struct {
	coord *Coordinator
	nodes []*node.Node
}

// startCluster boots N nodes on on basePort+i and a coordinator on basePort-10
func startCluster(t *testing.T, n, basePort int, reconcileFn ReconcileFunc) cluster {
	t.Helper()

	nodes := make([]*node.Node, n)
	for i := range nodes {
		addr := fmt.Sprintf(":%d", basePort+i)
		cfg := node.DefaultConfig(addr)
		cfg.MinLatency = 0
		cfg.MaxLatency = 0
		nodes[i] = node.New(fmt.Sprintf("node%d", i+1), cfg)
		if err := nodes[i].Start(); err != nil {
			t.Fatalf("failed to start node%d: %v", i+1, err)
		}
	}

	// Wire peer replication between nodes
	for i, nd := range nodes {
		for j, peer := range nodes {
			if i != j {
				nd.AddPeer(node.Peer{
					ID:   peer.ID(),
					Addr: fmt.Sprintf("http://localhost%s", peer.Config().Addr()),
				})
			}
		}
	}

	// Coordinator holds HTTP addresses only — no *node.Node references
	nodeAddrs := make([]node.Peer, n)
	for i, nd := range nodes {
		nodeAddrs[i] = node.Peer{
			ID:   nd.ID(),
			Addr: fmt.Sprintf("http://localhost%s", nd.Config().Addr()),
		}
	}

	q := DefaultQuorum(n)
	coordAddr := fmt.Sprintf(":%d", basePort-10)
	coord := New("coordinator", nodeAddrs, q, reconcileFn, coordAddr)
	if err := coord.Start(); err != nil {
		t.Fatalf("failed to start coordinator: %v", err)
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		coord.Stop(ctx)
		for _, nd := range nodes {
			nd.Stop(ctx)
		}
	})

	return cluster{coord: coord, nodes: nodes}
}

// injectConflict seeds concurrent versions directly into node stores,
// simulating what a network partition produces without going through coordinator.
func injectConflict(t *testing.T, nodes []*node.Node, key string,
	v1val string, v1clock clock.VectorClock,
	v2val string, v2clock clock.VectorClock,
) {
	t.Helper()
	v1 := storage.VersionedValue{Value: []byte(v1val), Clock: v1clock, NodeID: "node1", Timestamp: time.Now()}
	v2 := storage.VersionedValue{Value: []byte(v2val), Clock: v2clock, NodeID: "node2", Timestamp: time.Now().Add(time.Millisecond)}
	nodes[0].Store().Put(key, v1)
	nodes[1].Store().Put(key, v2)
	if len(nodes) > 2 {
		nodes[2].Store().Put(key, v1)
		nodes[2].Store().Put(key, v2)
	}
}

// ── HTTP helpers ──────────────────────────────────────────────────────────

const (
	PutTimeoutInSecs = 60 * time.Second
)

func coordPut(t *testing.T, coordAddr, key string, value []byte, baseClock clock.VectorClock) clientPutResponse {
	t.Helper()

	body, _ := json.Marshal(clientPutRequest{Value: value, BaseClock: baseClock})
	url := fmt.Sprintf("http://localhost%s/keys/%s", coordAddr, key)
	ctx, cancel := context.WithTimeout(context.Background(), PutTimeoutInSecs)
	defer cancel()

	resp, err := http.DefaultClient.Do(node.MustRequestWithCtx(ctx, http.MethodPut, url, body))
	if err != nil {
		t.Fatalf("coordPut failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("coordPut: unexpected status %d", resp.StatusCode)
	}

	var result clientPutResponse
	json.NewDecoder(resp.Body).Decode(&result)
	return result
}

func coordGet(t *testing.T, coordAddr, key string) clientGetResponse {
	t.Helper()
	url := fmt.Sprintf("http://localhost%s/keys/%s", coordAddr, key)

	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("coordGet failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return clientGetResponse{}
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("coordGet: unexpected status %d", resp.StatusCode)
	}

	var result clientGetResponse
	json.NewDecoder(resp.Body).Decode(&result)
	return result
}

// ── Tests ─────────────────────────────────────────────────────────────────

func TestHTTPQuorumWriteAndRead(t *testing.T) {
	cl := startCluster(t, 3, 19100, ReturnAll)

	r := coordPut(t, cl.coord.Addr(), "cart", []byte(`["shoes"]`), nil)
	if r.Acks < 2 {
		t.Fatalf("expected W=2 acks, got %d", r.Acks)
	}
	t.Logf("write clock=%v acks=%d", r.Clock, r.Acks)

	got := coordGet(t, cl.coord.Addr(), "cart")
	if got.HasConflict {
		t.Fatal("no conflict expected for single write")
	}
	if string(got.Siblings[0].Value) != `["shoes"]` {
		t.Fatalf("unexpected value: %s", got.Siblings[0].Value)
	}
}

func TestHTTPCausalWrites(t *testing.T) {
	cl := startCluster(t, 3, 19100, ReturnAll)

	r1 := coordPut(t, cl.coord.Addr(), "cart", []byte(`["shoes"]`), nil)
	r2 := coordPut(t, cl.coord.Addr(), "cart", []byte(`["shoes","shirt"]`), r1.Clock)
	t.Logf("write1=%v write2=%v", r1.Clock, r2.Clock)

	got := coordGet(t, cl.coord.Addr(), "cart")
	if got.HasConflict {
		t.Fatal("causal writes must not conflict")
	}
	if string(got.Siblings[0].Value) != `["shoes","shirt"]` {
		t.Fatalf("expected updated value, got: %s", got.Siblings[0].Value)
	}
}

func TestHTTPReadRepairUnionMerge(t *testing.T) {
	cl := startCluster(t, 3, 19100, UnionMerge)

	injectConflict(t, cl.nodes, "cart",
		`["shoes","watch"]`, clock.VectorClock{"node1": 2},
		`["shoes","shirt"]`, clock.VectorClock{"node1": 1, "node2": 1},
	)

	got := coordGet(t, cl.coord.Addr(), "cart")
	if !got.ReadRepaired {
		t.Fatal("expected read repair to trigger")
	}
	if got.HasConflict {
		t.Fatal("conflict should be resolved after read repair")
	}

	var items []string
	json.Unmarshal(got.Siblings[0].Value, &items)
	itemSet := make(map[string]bool)
	for _, i := range items {
		itemSet[i] = true
	}
	for _, expected := range []string{"shoes", "watch", "shirt"} {
		if !itemSet[expected] {
			t.Errorf("union merge missing %q, got: %v", expected, items)
		}
	}
	t.Logf("reconciled: %s", got.Siblings[0].Value)

	// Wait for write-back to propagate then verify clean state
	time.Sleep(100 * time.Millisecond)
	got2 := coordGet(t, cl.coord.Addr(), "cart")
	if got2.HasConflict {
		t.Fatal("conflict should be gone after read repair propagated")
	}
	if len(got2.Siblings) != 1 {
		t.Fatal("only one reconciled value should exist")
	}
}

func TestHTTPReturnAllNoAutoRepair(t *testing.T) {
	cl := startCluster(t, 3, 19100, ReturnAll)

	injectConflict(t, cl.nodes, "cart",
		`["shoes"]`, clock.VectorClock{"node1": 1},
		`["shirt"]`, clock.VectorClock{"node2": 1},
	)

	got := coordGet(t, cl.coord.Addr(), "cart")
	if got.ReadRepaired {
		t.Fatal("ReturnAll should not auto-repair")
	}
	if !got.HasConflict {
		t.Fatal("conflict should be visible to client")
	}
	if len(got.Siblings) != 2 {
		t.Fatalf("expected 2 siblings, got %d", len(got.Siblings))
	}
	for i, sib := range got.Siblings {
		t.Logf("sibling[%d] clock=%v value=%s", i, sib.Clock, sib.Value)
	}
}

func TestHTTPReadRepairLastWriteWins(t *testing.T) {
	cl := startCluster(t, 3, 19100, LastWriteWins)

	injectConflict(t, cl.nodes, "session",
		`{"token":"abc"}`, clock.VectorClock{"node1": 1},
		`{"token":"xyz"}`, clock.VectorClock{"node2": 1},
	)

	got := coordGet(t, cl.coord.Addr(), "session")
	if !got.ReadRepaired {
		t.Fatal("expected read repair")
	}
	if string(got.Siblings[0].Value) != `{"token":"xyz"}` {
		t.Fatalf("LWW should pick latest timestamp, got: %s", got.Siblings[0].Value)
	}
}

func TestHTTPSequentialWritesNoConflict(t *testing.T) {
	cl := startCluster(t, 3, 19100, UnionMerge)

	var lastClock clock.VectorClock
	writes := []string{`["shoes"]`, `["shoes","shirt"]`, `["shoes","shirt","watch"]`}
	for _, v := range writes {
		r := coordPut(t, cl.coord.Addr(), "cart", []byte(v), lastClock)
		lastClock = r.Clock
		t.Logf("wrote %s clock=%v", v, r.Clock)
	}

	got := coordGet(t, cl.coord.Addr(), "cart")
	if got.HasConflict {
		t.Fatalf("sequential writes must not conflict, got %d siblings", len(got.Siblings))
	}
	if string(got.Siblings[0].Value) != `["shoes","shirt","watch"]` {
		t.Fatalf("unexpected final value: %s", got.Siblings[0].Value)
	}
}

func TestHTTPPartitionAndRepair(t *testing.T) {
	cl := startCluster(t, 3, 19100, UnionMerge)

	// Write common ancestor
	r0 := coordPut(t, cl.coord.Addr(), "cart", []byte(`["shoes"]`), nil)
	t.Logf("ancestor clock: %v", r0.Clock)

	// Partition — nodes stop replicating
	for _, nd := range cl.nodes {
		nd.SetDropRate(1.0)
	}

	// Inject concurrent writes from each side of the partition
	v1 := storage.VersionedValue{
		Value:     []byte(`["shoes","watch"]`),
		Clock:     r0.Clock.Increment("node1"),
		NodeID:    "node1",
		Timestamp: time.Now(),
	}
	v2 := storage.VersionedValue{
		Value:     []byte(`["shoes","shirt"]`),
		Clock:     r0.Clock.Increment("node2"),
		NodeID:    "node2",
		Timestamp: time.Now().Add(time.Millisecond),
	}
	cl.nodes[0].Store().Put("cart", v1)
	cl.nodes[1].Store().Put("cart", v2)
	cl.nodes[2].Store().Put("cart", v1)
	cl.nodes[2].Store().Put("cart", v2)
	t.Logf("partition: node1=%s node2=%s", v1.Value, v2.Value)

	// Heal partition
	for _, nd := range cl.nodes {
		nd.SetDropRate(0)
	}

	got := coordGet(t, cl.coord.Addr(), "cart")
	if !got.ReadRepaired {
		t.Logf("siblings: %v", got.Siblings)
		t.Fatal("expected coordinator to detect and repair conflict")
	}

	var items []string
	json.Unmarshal(got.Siblings[0].Value, &items)
	itemSet := make(map[string]bool)
	for _, i := range items {
		itemSet[i] = true
	}
	for _, expected := range []string{"shoes", "watch", "shirt"} {
		if !itemSet[expected] {
			t.Errorf("repaired value missing %q, got: %v", expected, items)
		}
	}
	t.Logf("repaired: %s clock=%v", got.Siblings[0].Value, got.Siblings[0].Clock)
}
