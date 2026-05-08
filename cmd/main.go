// cmd/main.go

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/harshjoeyit/vectorclock/internal/clock"
	"github.com/harshjoeyit/vectorclock/internal/coordinator"
	"github.com/harshjoeyit/vectorclock/internal/node"
	"github.com/harshjoeyit/vectorclock/internal/storage"
)

const (
	basePort = 19100
)

func printSection(title string) {
	fmt.Printf("\n=== %s ===\n\n", title)
}

func dumpNodes(nodes []*node.Node, key string) {
	for _, nd := range nodes {
		r := nd.Store().Get(key)

		if !r.Found {
			fmt.Printf("%s -> no data\n", nd.ID())
			continue
		}

		if r.HasConflict {
			fmt.Printf("%s -> %d siblings\n", nd.ID(), len(r.Siblings))
			for i, sib := range r.Siblings {
				fmt.Printf("  [%d] clock=%s value=%s\n", i, sib.Clock, sib.Value)
			}
			continue
		}

		fmt.Printf("%s -> clock=%s value=%s\n",
			nd.ID(),
			r.Siblings[0].Clock,
			r.Siblings[0].Value,
		)
	}
}

func makeCluster(reconcileFn coordinator.ReconcileFunc) (*coordinator.Coordinator, []*node.Node) {
	nodes := make([]*node.Node, 3)

	for i := range nodes {
		addr := fmt.Sprintf(":%d", basePort+i)
		cfg := node.DefaultConfig(addr)
		cfg.MinLatency = 0
		cfg.MaxLatency = 0

		nodes[i] = node.New(fmt.Sprintf("node%d", i+1), cfg)

		if err := nodes[i].Start(); err != nil {
			fmt.Fprintf(os.Stderr, "failed to start node%d: %v\n", i+1, err)
			os.Exit(1)
		}
	}

	for i, nd := range nodes {
		for j, peer := range nodes {
			if i == j {
				continue
			}

			nd.AddPeer(node.Peer{
				ID:   peer.ID(),
				Addr: fmt.Sprintf("http://localhost%s", peer.Config().Addr()),
			})
		}
	}

	nodeAddrs := make([]node.Peer, len(nodes))

	for i, nd := range nodes {
		nodeAddrs[i] = node.Peer{
			ID:   nd.ID(),
			Addr: fmt.Sprintf("http://localhost%s", nd.Config().Addr()),
		}
	}

	q := coordinator.DefaultQuorum(len(nodes))
	addr := fmt.Sprintf(":%d", basePort-10)
	coord := coordinator.New(
		"coordinator",
		nodeAddrs,
		q,
		reconcileFn,
		addr,
	)

	if err := coord.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to start coordinator: %v\n", err)
		os.Exit(1)
	}

	return coord, nodes
}

func scenario1() {
	printSection("Scenario 1: Linear Writes")

	coord, nodes := makeCluster(coordinator.ReturnAll)
	ctx := context.Background()

	writes := []string{
		`["shoes"]`,
		`["shoes","shirt"]`,
		`["shoes","shirt","watch"]`,
	}

	for _, v := range writes {
		fmt.Printf("writing %s\n", v)

		r, err := coord.Put(ctx, "cart", []byte(v), nil)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Put failed: %v\n", err)
			return
		}

		fmt.Printf("clock=%s\n", r.Clock)
	}

	fmt.Println()
	dumpNodes(nodes, "cart")

	r, _ := coord.Get(ctx, "cart")

	fmt.Println()

	if r.HasConflict {
		fmt.Printf("unexpected conflict: %d siblings\n", len(r.Siblings))
		return
	}

	fmt.Printf("clean read: %s\n", r.Siblings[0].Value)
}

func scenario2() {
	printSection("Scenario 2: Network Partition")

	_, nodes := makeCluster(coordinator.ReturnAll)

	base := storage.VersionedValue{
		Value:     []byte(`["shoes"]`),
		Clock:     clock.VectorClock{"node1": 1},
		NodeID:    "node1",
		Timestamp: time.Now(),
	}

	for _, nd := range nodes {
		nd.Store().Put("cart", base)
	}

	fmt.Printf("base version: clock=%s value=%s\n\n", base.Clock, base.Value)

	phoneWrite := storage.VersionedValue{
		Value:     []byte(`["shoes","watch"]`),
		Clock:     clock.VectorClock{"node1": 2},
		NodeID:    "node1",
		Timestamp: time.Now(),
	}

	laptopWrite := storage.VersionedValue{
		Value:     []byte(`["shoes","shirt"]`),
		Clock:     clock.VectorClock{"node1": 1, "node2": 1},
		NodeID:    "node2",
		Timestamp: time.Now().Add(50 * time.Millisecond),
	}

	nodes[0].Store().Put("cart", phoneWrite)
	nodes[1].Store().Put("cart", laptopWrite)

	fmt.Printf("phone write  -> clock=%s value=%s\n",
		phoneWrite.Clock,
		phoneWrite.Value,
	)

	fmt.Printf("laptop write -> clock=%s value=%s\n\n",
		laptopWrite.Clock,
		laptopWrite.Value,
	)

	result := nodes[0].Store().Put("cart", laptopWrite)

	fmt.Printf("replication result: %s\n\n", result)

	dumpNodes(nodes, "cart")

	fmt.Println()

	rel := phoneWrite.Clock.Compare(laptopWrite.Clock)

	fmt.Printf("phone=%s\n", phoneWrite.Clock)
	fmt.Printf("laptop=%s\n", laptopWrite.Clock)
	fmt.Printf("relation=%s\n", rel)
}

func scenario3() {
	printSection("Scenario 3: Read Repair")

	coord, nodes := makeCluster(coordinator.UnionMerge)
	ctx := context.Background()

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

	fmt.Println("before repair:")
	dumpNodes(nodes, "cart")

	fmt.Println()

	result, err := coord.Get(ctx, "cart")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Get failed: %v\n", err)
		return
	}

	if result.ReadRepaired {
		fmt.Println("read repair triggered")
		fmt.Printf("value=%s\n", result.Siblings[0].Value)
		fmt.Printf("clock=%s\n", result.Siblings[0].Clock)
	}

	time.Sleep(50 * time.Millisecond)

	fmt.Println()
	fmt.Println("after repair:")

	dumpNodes(nodes, "cart")

	fmt.Println()

	rc := result.Siblings[0].Clock

	fmt.Printf("reconciled=%s\n", rc)
	fmt.Printf("dominates v1=%v\n", rc.Dominates(v1.Clock))
	fmt.Printf("dominates v2=%v\n", rc.Dominates(v2.Clock))
}

func scenario4() {
	printSection("Scenario 4: Stale Replication")

	coord, nodes := makeCluster(coordinator.ReturnAll)
	ctx := context.Background()

	r1, _ := coord.Put(
		ctx,
		"cart",
		[]byte(`["shoes"]`),
		nil,
	)

	fmt.Printf("v1 clock=%s\n", r1.Clock)

	r2, _ := coord.Put(
		ctx,
		"cart",
		[]byte(`["shoes","shirt"]`),
		r1.Clock,
	)

	fmt.Printf("v2 clock=%s\n\n", r2.Clock)

	dumpNodes(nodes, "cart")

	fmt.Println()

	stale := storage.VersionedValue{
		Value:     []byte(`["shoes"]`),
		Clock:     r1.Clock,
		NodeID:    "node1",
		Timestamp: time.Now(),
	}

	result := nodes[0].Store().Put("cart", stale)

	fmt.Printf("stale replication result: %s\n\n", result)

	dumpNodes(nodes, "cart")

	fmt.Println()

	rel := r1.Clock.Compare(r2.Clock)

	fmt.Printf("v1=%s\n", r1.Clock)
	fmt.Printf("v2=%s\n", r2.Clock)
	fmt.Printf("relation=%s\n", rel)
}

func clockTable() {
	printSection("Clock Comparison Rules")

	type row struct {
		a    clock.VectorClock
		b    clock.VectorClock
		desc string
	}

	rows := []row{
		{
			clock.VectorClock{"A": 1},
			clock.VectorClock{"A": 2},
			"ancestor",
		},
		{
			clock.VectorClock{"A": 2},
			clock.VectorClock{"A": 1},
			"descendant",
		},
		{
			clock.VectorClock{"A": 2},
			clock.VectorClock{"A": 1, "B": 1},
			"concurrent",
		},
		{
			clock.VectorClock{"A": 1},
			clock.VectorClock{"A": 1},
			"equal",
		},
	}

	for _, r := range rows {
		fmt.Printf(
			"%s  vs  %s  =>  %s (%s)\n",
			r.a,
			r.b,
			r.a.Compare(r.b),
			r.desc,
		)
	}

	fmt.Println()

	vc := clock.VectorClock{
		"node1":       3,
		"node2":       1,
		"coordinator": 2,
	}

	b, _ := json.MarshalIndent(vc, "", "  ")

	fmt.Println("json:")
	fmt.Println(string(b))
}

func main() {
	fmt.Println("Vector Clock Demo")

	scenario1()
	// scenario2()
	// scenario3()
	// scenario4()
	// clockTable()

	fmt.Println("\ndone")
}
