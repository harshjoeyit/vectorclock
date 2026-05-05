// cmd/main.go — Vector Clock demo
//
// Runs 4 scenarios that mirror real DynamoDB behaviour:
//
//	Scenario 1: Linear writes — clean causal chain, no conflict
//	Scenario 2: Network partition — concurrent writes produce siblings
//	Scenario 3: Read repair — coordinator detects and resolves conflict
//	Scenario 4: Stale replication — late message is silently discarded
package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/harshjoeyit/vectorclock/internal/clock"
	"github.com/harshjoeyit/vectorclock/internal/coordinator"
	"github.com/harshjoeyit/vectorclock/internal/node"
	"github.com/harshjoeyit/vectorclock/internal/storage"
)

const (
	reset  = "\033[0m"
	bold   = "\033[1m"
	red    = "\033[31m"
	green  = "\033[32m"
	yellow = "\033[33m"
	cyan   = "\033[36m"
	gray   = "\033[90m"
)

func header(s string) {
	fmt.Printf("\n%s%s━━━ %s %s━━━%s\n\n", bold, cyan, s, strings.Repeat("━", max(0, 50-len(s))), reset)
}

func step(format string, args ...any) {
	fmt.Printf("  %s▶%s  "+format+"\n", append([]any{yellow, reset}, args...)...)
}

func ok(format string, args ...any) {
	fmt.Printf("  %s✓%s  "+format+"\n", append([]any{green, reset}, args...)...)
}

func conflict(format string, args ...any) {
	fmt.Printf("  %s⚡%s  "+format+"\n", append([]any{red, reset}, args...)...)
}

func info(format string, args ...any) {
	fmt.Printf("  %s·%s  "+format+"\n", append([]any{gray, reset}, args...)...)
}

func dumpNodes(nodes []*node.Node, key string) {
	for _, nd := range nodes {
		r := nd.Store().Get(key)
		if !r.Found {
			info("node=%-6s  <no data>", nd.ID())
			continue
		}
		if r.HasConflict {
			conflict("node=%-6s  %d SIBLINGS:", nd.ID(), len(r.Siblings))
			for i, sib := range r.Siblings {
				info("           [%d] clock=%-28s value=%s", i, sib.Clock, sib.Value)
			}
		} else {
			ok("node=%-6s  clock=%-28s value=%s", nd.ID(), r.Siblings[0].Clock, r.Siblings[0].Value)
		}
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// ── cluster bootstrap ─────────────────────────────────────────────────────

func makeCluster(basePort int, reconcileFn coordinator.ReconcileFunc) (*coordinator.Coordinator, []*node.Node) {
	nodes := make([]*node.Node, 3)
	for i := range nodes {
		addr := fmt.Sprintf(":%d", basePort+i)
		cfg := node.DefaultConfig(addr)
		cfg.MinLatency = 0
		cfg.MaxLatency = 0
		nodes[i] = node.New(fmt.Sprintf("node%d", i+1), cfg)
	}
	q := coordinator.DefaultQuorum(len(nodes))
	coord := coordinator.New("coordinator", nodes, q, reconcileFn)
	return coord, nodes
}

// ─────────────────────────────────────────────────────────────────────────
// Scenario 1: Linear writes
// ─────────────────────────────────────────────────────────────────────────

func scenario1() {
	header("Scenario 1: Linear Writes — No Conflict")
	fmt.Println("  User adds items to cart sequentially.")
	fmt.Println("  Each write knows about the previous one → clean causal chain.\n")

	coord, nodes := makeCluster(19100, coordinator.ReturnAll)
	ctx := context.Background()

	writes := []string{`["shoes"]`, `["shoes","shirt"]`, `["shoes","shirt","watch"]`}
	for _, v := range writes {
		step("Writing: %s", v)
		r, err := coord.Put(ctx, "cart", []byte(v))
		if err != nil {
			fmt.Fprintf(os.Stderr, "Put failed: %v\n", err)
			return
		}
		ok("Accepted  clock=%s", r.Clock)
	}

	fmt.Println()
	step("State across all nodes:")
	dumpNodes(nodes, "cart")

	r, _ := coord.Get(ctx, "cart")
	fmt.Println()
	if r.HasConflict {
		conflict("Unexpected conflict — %d siblings", len(r.Siblings))
	} else {
		ok("Clean read: %s", r.Siblings[0].Value)
	}
}

func scenario2() {
	header("Scenario 2: Network Partition → Concurrent Siblings")
	fmt.Println("  User has cart open on phone AND laptop simultaneously.")
	fmt.Println("  Partition means neither write sees the other.\n")

	_, nodes := makeCluster(19100, coordinator.ReturnAll)

	// Common ancestor — both clients last saw this version
	base := storage.VersionedValue{
		Value:     []byte(`["shoes"]`),
		Clock:     clock.VectorClock{"node1": 1},
		NodeID:    "node1",
		Timestamp: time.Now(),
	}
	for _, nd := range nodes {
		nd.Store().Put("cart", base)
	}
	step("Common ancestor written to all nodes: clock=%s value=%s", base.Clock, base.Value)

	// Partition starts — node1 and node2 write independently
	fmt.Println()
	step("── Partition begins — nodes stop replicating ──")
	fmt.Println()

	phoneWrite := storage.VersionedValue{
		Value:     []byte(`["shoes","watch"]`),
		Clock:     clock.VectorClock{"node1": 2}, // only saw node1:1, incremented node1
		NodeID:    "node1",
		Timestamp: time.Now(),
	}
	laptopWrite := storage.VersionedValue{
		Value:     []byte(`["shoes","shirt"]`),
		Clock:     clock.VectorClock{"node1": 1, "node2": 1}, // only saw node1:1, incremented node2
		NodeID:    "node2",
		Timestamp: time.Now().Add(50 * time.Millisecond),
	}

	nodes[0].Store().Put("cart", phoneWrite)
	step("Phone  → node1: clock=%s value=%s", phoneWrite.Clock, phoneWrite.Value)

	nodes[1].Store().Put("cart", laptopWrite)
	step("Laptop → node2: clock=%s value=%s", laptopWrite.Clock, laptopWrite.Value)

	fmt.Println()
	step("── Partition heals — node2 replicates to node1 ──")
	fmt.Println()

	result := nodes[0].Store().Put("cart", laptopWrite)
	fmt.Printf("  Store result on node1: %s%s%s\n\n", red, result, reset)

	step("State across all nodes:")
	dumpNodes(nodes, "cart")

	fmt.Println()
	info("Clock comparison:")
	r1 := phoneWrite.Clock.Compare(laptopWrite.Clock)
	info("  phone=%s  laptop=%s  relation=%s%s%s",
		phoneWrite.Clock, laptopWrite.Clock, red, r1, reset)
	info("  Neither dominates the other → both must be retained as siblings")
}

// ─────────────────────────────────────────────────────────────────────────
// Scenario 3: Read repair
// ─────────────────────────────────────────────────────────────────────────

func scenario3() {
	header("Scenario 3: Read Repair — Coordinator Resolves Conflict")
	fmt.Println("  Same partition as Scenario 2, but now the coordinator")
	fmt.Println("  detects conflict on read and automatically reconciles.\n")

	coord, nodes := makeCluster(19100, coordinator.UnionMerge)
	ctx := context.Background()

	// Inject the same conflict directly into stores
	v1 := storage.VersionedValue{
		Value:     []byte(`["shoes","watch"]`),
		Clock:     clock.VectorClock{"node1": 2},
		NodeID:    "node1",
		Timestamp: time.Now(),
	}
	v2 := storage.VersionedValue{
		Value:     []byte(`["shoes","shirt"]`),
		Clock:     clock.VectorClock{"node1": 1, "node2": 1},
		NodeID:    "node2",
		Timestamp: time.Now().Add(time.Millisecond),
	}
	nodes[0].Store().Put("cart", v1)
	nodes[1].Store().Put("cart", v2)
	nodes[2].Store().Put("cart", v1)
	nodes[2].Store().Put("cart", v2)

	step("Pre-repair state:")
	dumpNodes(nodes, "cart")

	fmt.Println()
	step("Client reads cart...")
	result, err := coord.Get(ctx, "cart")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Get failed: %v\n", err)
		return
	}

	fmt.Println()
	if result.ReadRepaired {
		ok("Read repair triggered!")
		ok("Reconciled value : %s", result.Siblings[0].Value)
		ok("Reconciled clock : %s", result.Siblings[0].Clock)
		info("(clock is MAX of all siblings — dominates both)")
	}

	// Wait for write-back goroutines
	time.Sleep(50 * time.Millisecond)

	fmt.Println()
	step("Post-repair state:")
	dumpNodes(nodes, "cart")

	fmt.Println()
	step("Verify reconciled clock dominates both siblings:")
	rc := result.Siblings[0].Clock
	info("reconciled=%s", rc)
	info("sibling v1=%s  dominated=%v", v1.Clock, rc.Dominates(v1.Clock))
	info("sibling v2=%s  dominated=%v", v2.Clock, rc.Dominates(v2.Clock))
}

// ─────────────────────────────────────────────────────────────────────────
// Scenario 4: Stale replication — late message discarded
// ─────────────────────────────────────────────────────────────────────────

func scenario4() {
	header("Scenario 4: Stale Replication — Late Message Discarded")
	fmt.Println("  A replication message for an old version arrives")
	fmt.Println("  after a newer version is already present.\n")

	coord, nodes := makeCluster(19100, coordinator.ReturnAll)
	ctx := context.Background()

	step("Writing v1: [\"shoes\"]")
	r1, _ := coord.Put(ctx, "cart", []byte(`["shoes"]`))
	ok("v1 clock: %s", r1.Clock)

	step("Writing v2: [\"shoes\",\"shirt\"]  (causal successor of v1)")
	r2, _ := coord.Put(ctx, "cart", []byte(`["shoes","shirt"]`))
	ok("v2 clock: %s", r2.Clock)

	fmt.Println()
	step("State after v2:")
	dumpNodes(nodes, "cart")

	// Simulate a delayed replication of v1 arriving on node1
	fmt.Println()
	step("Late replication arrives: v1 clock=%s", r1.Clock)
	stale := storage.VersionedValue{
		Value:     []byte(`["shoes"]`),
		Clock:     r1.Clock,
		NodeID:    "node1",
		Timestamp: time.Now(),
	}
	result := nodes[0].Store().Put("cart", stale)
	fmt.Printf("  Store result: %s%s%s\n", yellow, result, reset)

	fmt.Println()
	step("State after stale write attempt:")
	dumpNodes(nodes, "cart")

	fmt.Println()
	info("v1 clock=%s", r1.Clock)
	info("v2 clock=%s", r2.Clock)
	rel := r1.Clock.Compare(r2.Clock)
	info("v1 relation to v2: %s → v1 is an ancestor, safe to discard", rel)
}

func main() {
	scenario1()
	// scenario2()
	// scenario3()
	// scenario4()
}
