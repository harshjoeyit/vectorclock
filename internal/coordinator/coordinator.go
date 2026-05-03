// Package coordinator implements quorum-based reads and writes,
// conflict detection, and pluggable reconciliation strategies.
package coordinator

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/harshjoeyit/vectorclock/internal/clock"
	"github.com/harshjoeyit/vectorclock/internal/node"
	"github.com/harshjoeyit/vectorclock/internal/storage"
)

// QuorumConfig defines W and R quorum sizes relative to the cluster of N nodes.
//
// Dynamo's consistency guarantee: if W + R > N, reads always see the latest write.
// Common configurations:
//
//	W=N, R=1 - strong write consistency, fast reads
//	W=1, R=N - fast writes, strong read consistency
//	W=2, R=2 - balanced (N=3), Reads and writes must overlap on at least one node. Tolerates 1 node failure for both
type QuorumConfig struct {
	N int // total nodes
	W int // write quoram - minimum nodes that must ACK a write
	R int // read quoram - minimum nodes that must respond to a read
}

// DefaultQuorum returns W=2, R=2, N=3 — the Dynamo paper's recommended default.
func DefaultQuorum(n int) QuorumConfig {
	w := n/2 + 1
	return QuorumConfig{N: n, W: w, R: w}
}

// WriteResult is returned to the caller after a coordinated write.
type WriteResult struct {
	Clock  clock.VectorClock
	Acks   int    // how many nodes acknowledged
	NodeID string // which node was the coordinator
}

// ReadResult is returned to the caller after a coordinated read.
type ReadResult struct {
	Siblings    []storage.VersionedValue
	HasConflict bool
	Acks        int
	// ReadRepaired is true if the coordinator detected and resolved a conflict
	// during this read, writing a reconciled version back automatically.
	ReadRepaired bool
}

// Coordinator fans writes and reads across a set of nodes,
// enforcing quorum and performing read repair on conflict detection.
type Coordinator struct {
	id        string
	nodes     []*node.Node
	quorum    QuorumConfig
	reconcile ReconcileFunc
	logger    *log.Logger
}

// New creates a coordinator.
func New(id string, nodes []*node.Node, q QuorumConfig, reconcileFn ReconcileFunc) *Coordinator {
	return &Coordinator{
		id:        id,
		nodes:     nodes,
		quorum:    q,
		reconcile: reconcileFn,
		logger:    log.New(log.Writer(), fmt.Sprintf("[coord:%s] ", id), log.LstdFlags|log.Lmsgprefix),
	}
}

// Put writes a value for key across W nodes.
//
// Algorithm:
//  1. Read current versions from R nodes to build the most up-to-date clock.
//     This prevents the coordinator from issuing a write with a stale clock.
//  2. Build the new versioned value: merge all known clocks, increment ours.
//  3. Fan out the write to all N nodes concurrently.
//  4. Wait for W ACKs. If W ACKs arrive → success.
//     Remaining goroutines continue in the background (best-effort replication).
func (c *Coordinator) Put(ctx context.Context, key string, value []byte) (WriteResult, error) {
	c.logger.Printf("calling PUT key=%q, value=%s", key, value)
	// Step 1: pre-read to get the latest known clock for this key.
	// Without this, two coordinators writing concurrently would both start
	// from clock.New() and create a false conflict even on sequential logical writes.
	latestClock := c.gatherLatestClock(ctx, key)
	latestClock = latestClock.Increment(c.id)

	// vv is the version that coordinator writes to all node
	vv := storage.VersionedValue{
		Value:     value,
		Clock:     latestClock,
		NodeID:    c.id,
		Timestamp: time.Now(),
	}

	// Step 2: fan-out write to all nodes, wait for W ACKs.
	ackCh := make(chan ackMsg, len(c.nodes))

	for _, nd := range c.nodes {
		go func(n *node.Node) {
			result := n.Store().Put(key, vv)
			if result == storage.PutStale {
				ackCh <- ackMsg{fmt.Errorf("node %s rejected write as stale", n.ID())}
				return
			}
			// ACK received
			c.logger.Printf("PUT key=%q → node=%s result=%s", key, n.ID(), result)
			ackCh <- ackMsg{nil}
		}(nd)
	}

	acks, errs := c.waitForQuorum(ctx, ackCh, c.quorum.N, c.quorum.W)
	if errs != nil {
		c.logger.Printf("PUT FAILED key=%q", key)
		return WriteResult{}, fmt.Errorf("put faile: %v", errors.Join(errs...))
	}

	if acks < c.quorum.W {
		return WriteResult{}, fmt.Errorf("write quorum not reached: got %d/%d acks, errors: %v",
			acks, c.quorum.W, errs)
	}

	c.logger.Printf("PUT key=%q clock=%s", key, latestClock)

	return WriteResult{
		Clock:  latestClock,
		Acks:   acks,
		NodeID: c.id,
	}, nil
}

// Get reads key from R nodes, detects conflicts, and applies read repair.
//
// Algorithm:
//  1. Fan out GET to all N nodes concurrently.
//  2. Wait for R responses.
//  3. Merge all returned versions — prune dominated ones, keep concurrent siblings.
//  4. If siblings remain (conflict):
//     a. Call reconcileFn to produce a resolved version.
//     b. If reconcileFn returns a non-sentinel value, write it back to ALL nodes
//     (read repair) so future reads see a clean state.
//  5. Return result to caller.
func (c *Coordinator) Get(ctx context.Context, key string) (ReadResult, error) {
	c.logger.Printf("calling GET key=%q", key)

	var allVersions []storage.VersionedValue
	var mu sync.Mutex

	// Step 1: fan-out reads to all nodes, wait for R ACKs.
	ackCh := make(chan ackMsg, len(c.nodes))

	ctxWT, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	for _, nd := range c.nodes {
		go func(n *node.Node) {
			result := n.Store().Get(key)
			if !result.Found {
				return
			}

			mu.Lock()
			allVersions = append(allVersions, result.Siblings...)
			mu.Unlock()

			c.logger.Printf("GET key=%q → node=%s result=%+v", key, n.ID(), result)
			ackCh <- ackMsg{nil}
		}(nd)
	}

	acks, errs := c.waitForQuorum(ctxWT, ackCh, c.quorum.N, c.quorum.R)
	if errs != nil {
		c.logger.Printf("GET key=%q FAILED", key)
		return ReadResult{}, fmt.Errorf("get failed: %+v", errors.Join(errs...))
	}

	if acks < c.quorum.R {
		return ReadResult{}, fmt.Errorf("read quorum not reached: got %d/%d acks, errors: %v",
			acks, c.quorum.W, errs)
	}

	// Key not found
	if len(allVersions) == 0 {
		return ReadResult{}, fmt.Errorf("key not found: %s", key)
	}

	// Step 2: Prune any version dominated by another. What remains are true siblings.
	pruned := pruneDominated(allVersions)

	result := ReadResult{
		Siblings:    pruned,
		HasConflict: len(pruned) > 1,
		Acks:        acks,
	}

	// Step 3: Read repair if there's a conflict.
	if result.HasConflict {
		c.logger.Printf("GET key=%q CONFLICT detected (%d siblings) — attempting read repair",
			key, len(pruned))

		resolved := c.reconcile(key, pruned)

		// ReturnAll returns a zero VersionedValue — skip write-back.
		if resolved.Clock != nil {
			// Validate: resolved clock must dominate ALL siblings.
			for _, sib := range pruned {
				if !sib.IsDominatedBy(resolved) {
					c.logger.Printf("GET key=%q reconcile produced non-dominant clock — skipping repair", key)
					return result, nil
				}
			}

			c.logger.Printf("GET key=%q read repair → writing back clock=%s", key, resolved.Clock)
			c.writeBack(ctx, key, resolved)

			// Update result
			result.ReadRepaired = true
			result.Siblings = []storage.VersionedValue{resolved}
			result.HasConflict = false
		}
	}

	return result, nil
}

// --- Internal helpers ----------------------------------------------------

// gatherLatestClock reads from all nodes and returns the merged clock for key.
// This gives the coordinator the most up-to-date causal context before writing.
func (c *Coordinator) gatherLatestClock(ctx context.Context, key string) clock.VectorClock {
	merged := clock.New()
	var mu sync.Mutex

	var wg sync.WaitGroup
	for _, nd := range c.nodes {
		wg.Add(1)
		go func(n *node.Node) {
			defer wg.Done()
			res := n.Store().Get(key)
			// key does not exist on this node
			if !res.Found {
				return
			}

			local := clock.New()
			for _, sib := range res.Siblings {
				local = local.Merge(sib.Clock)
			}

			mu.Lock()
			merged = merged.Merge(local)
			mu.Unlock()
		}(nd)
	}

	wg.Wait()
	return merged
}

// writeBack writes a reconciled version to all nodes (best-effort).
// Failures are logged but do not cause the read to fail — the reconciled
// value has already been returned to the caller.
func (c *Coordinator) writeBack(ctx context.Context, key string, resolved storage.VersionedValue) {
	for _, nd := range c.nodes {
		go func(n *node.Node) {
			select {
			case <-ctx.Done():
				c.logger.Printf("READ REPAIR key=%q → node=%s CANCELLED", key, n.ID())
				return
			default:
			}

			result := n.Store().Put(key, resolved)
			c.logger.Printf("READ REPAIR key=%q → node=%s OK result=%s", key, n.ID(), result)
		}(nd)
	}
}

// pruneDominated returns only the versions NOT dominated by any other version
// in the set. These are the true concurrent siblings.
func pruneDominated(versions []storage.VersionedValue) []storage.VersionedValue {
	survivors := make([]storage.VersionedValue, 0, len(versions))

	for i, candidate := range versions {
		// if candidate is dominated by at least one other version, it doesn't survive
		isDominated := false
		for j, other := range versions {
			if i == j {
				continue
			}
			r := candidate.Clock.Compare(other.Clock)
			// A version is pruned only if strictly dominated (HappensBefore), hence
			// cannot use IsDominatedBy() which returns true if clocks are equal
			// Equal clocks = same version replicated to multiple nodes — so keep one.
			// We keep the first occurrence (i < j) to deduplicate Equal versions.
			if r == clock.HappensBefore {
				isDominated = true
				break
			}
			if r == clock.Equal && i > j {
				isDominated = true
				break
			}
		}

		if !isDominated {
			survivors = append(survivors, candidate)
		}
	}
	return survivors
}

// waitForQuorum collects from ackCh until `need` successes or `total` responses.
// Returns (successCount, errorList).
type ackMsg struct{ err error }

func (c *Coordinator) waitForQuorum(ctx context.Context, ackCh <-chan ackMsg, total, need int) (int, []error) {
	acks := 0
	var errs []error
	received := 0
	deadline := time.After(2 * time.Second)

	for received < total && acks < need {
		select {
		case a := <-ackCh:
			received++
			if a.err != nil {
				errs = append(errs, a.err)
			} else {
				acks++
			}
		case <-deadline:
			return acks, append(errs, fmt.Errorf("quorum timout"))
		case <-ctx.Done():
			return acks, append(errs, ctx.Err())
		}
	}

	return acks, errs
}
