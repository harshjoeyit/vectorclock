// Package coordinator implements quorum-based reads and writes,
// conflict detection, and pluggable reconciliation strategies.
//
// The coordinator is itself an HTTP server — it exposes the same
// PUT /keys/{key} and GET /keys/{key} interface to clients, and internally
// fans requests out to nodes via their internal HTTP endpoints
//
// This draws parallels with Dynamo architecture: the coordinator is just
// a node that happens to be handling a particular request.
package coordinator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/harshjoeyit/vectorclock/internal/clock"
	"github.com/harshjoeyit/vectorclock/internal/node"
	"github.com/harshjoeyit/vectorclock/internal/storage"
)

// ackMsg carries the result of a single node write attempt.
type ackMsg struct{ err error }

// NodeClient is a lightweight HTTP client for a single node.
// The coordinator holds a slice of these — one per node in the preference list.
type NodeClient struct {
	ID   string
	Addr string // e.g. "http://localhost:8081"
	http *http.Client
}

func newNodeClient(id, addr string) NodeClient {
	return NodeClient{
		ID:   id,
		Addr: addr,
		http: &http.Client{Timeout: 2 * time.Second},
	}
}

// replicate sends a versioned value to a node via POST /internal/replicate/{key}.
// This is the same endpoint peer nodes use — the coordinator is just another caller.
func (nc NodeClient) replicate(ctx context.Context, key string, vv storage.VersionedValue) error {
	log.Printf("calling /internal/replicate/ key=%q, node=%s", key, nc.ID)

	wv := node.WireValue{
		Value:    vv.Value,
		Clock:    vv.Clock,
		NodeID:   vv.NodeID,
		WallNano: vv.Timestamp.UnixNano(),
	}
	body, _ := json.Marshal(wv)

	url := fmt.Sprintf("%s/internal/replicate/%s", nc.Addr, key)

	resp, err := nc.http.Do(node.MustRequestWithCtx(ctx, http.MethodPost, url, body))
	if err != nil {
		return fmt.Errorf("node %s unreachable: %w", nc.ID, err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("node %s replicate: unexpected status %d", nc.ID, resp.StatusCode)
	}
	return nil
}

// versions fetches raw siblings from a node via GET /internal/versions/{key}.
func (nc NodeClient) versions(ctx context.Context, key string) ([]storage.VersionedValue, error) {
	log.Printf("calling /internal/versions/ key=%q, node=%s", key, nc.ID)

	url := fmt.Sprintf("%s/internal/versions/%s", nc.Addr, key)
	resp, err := nc.http.Do(node.MustRequestWithCtx(ctx, http.MethodGet, url, nil))
	if err != nil {
		return nil, fmt.Errorf("node %s unreachable: %w", nc.ID, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, nil // key not present on this node yet — not an error
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("node %s versions: unexpected status %d", nc.ID, resp.StatusCode)
	}

	var wvs []node.WireValue
	if err := json.NewDecoder(resp.Body).Decode(&wvs); err != nil {
		return nil, fmt.Errorf("node %s versions: decode: %w", nc.ID, err)
	}

	vvs := make([]storage.VersionedValue, len(wvs))
	for i, wv := range wvs {
		vvs[i] = wv.ToVersionedValue()
	}
	return vvs, nil
}

// ── Quorum config ─────────────────────────────────────────────────────────

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

// ── Coordinator ───────────────────────────────────────────────────────────

// Coordinator is an HTTP server fans writes and reads across a set of nodes,
// enforcing quorum and performing read repair on conflict detection.
type Coordinator struct {
	id        string
	nodes     []NodeClient
	quorum    QuorumConfig
	reconcile ReconcileFunc
	addr      string
	server    *http.Server
	logger    *log.Logger
}

func New(id string, nodeAddrs []node.Peer, q QuorumConfig, reconcileFn ReconcileFunc, addr string) *Coordinator {
	clients := make([]NodeClient, len(nodeAddrs))
	for i, n := range nodeAddrs {
		clients[i] = newNodeClient(n.ID, n.Addr)
	}
	return &Coordinator{
		id:        id,
		nodes:     clients,
		quorum:    q,
		reconcile: reconcileFn,
		addr:      addr,
		logger:    log.New(log.Writer(), fmt.Sprintf("[coord:%s] ", id), log.LstdFlags|log.Lmsgprefix),
	}
}

// ── Lifecycle ─────────────────────────────────────────────────────────────

// Start boots the coordinator's HTTP server.
func (c *Coordinator) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /keys/{key}", c.handlePut)
	mux.HandleFunc("GET /keys/{key}", c.handleGet)

	ln, err := net.Listen("tcp", c.addr)
	if err != nil {
		return fmt.Errorf("coordinator %s: listen: %w", c.id, err)
	}

	c.server = &http.Server{
		Handler: mux,
	}

	go func() {
		if err := c.server.Serve(ln); err != nil && err != http.ErrServerClosed {
			c.logger.Printf("server error: %v", err)
		}
	}()

	c.logger.Printf("listening on %s", c.addr)
	return nil
}

func (c *Coordinator) Addr() string {
	return c.addr
}

// Stop gracefully shuts down the node.
func (c *Coordinator) Stop(ctx context.Context) error {
	if c.server == nil {
		return nil
	}
	return c.server.Shutdown(ctx)
}

// ── HTTP handlers ─────────────────────────────────────────────────────────

// clientPutRequest is what a client sends to the coordinator.
type clientPutRequest struct {
	Value     []byte            `json:"value"`
	BaseClock clock.VectorClock `json:"base_clock,omitempty"`
}

// clientPutResponse is returned to the client after a write.
type clientPutResponse struct {
	Clock  clock.VectorClock `json:"clock"`
	Acks   int               `json:"acks"`
	NodeID string            `json:"node_id"`
}

// clientGetResponse is returned to the client after a read.
type clientGetResponse struct {
	HasConflict  bool             `json:"has_conflict"`
	ReadRepaired bool             `json:"read_repaired"`
	Siblings     []node.WireValue `json:"siblings"`
}

func (c *Coordinator) handlePut(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")

	var req clientPutRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request body", http.StatusBadRequest)
		return
	}

	result, err := c.Put(r.Context(), key, req.Value, req.BaseClock)
	if err != nil {
		c.logger.Printf("PUT key=%q error: %v", key, err)
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(clientPutResponse{
		Clock:  result.Clock,
		Acks:   result.Acks,
		NodeID: result.NodeID,
	})
}

func (c *Coordinator) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")

	result, err := c.Get(r.Context(), key)
	if err != nil {
		c.logger.Printf("GET key=%q error: %v", key, err)
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	resp := clientGetResponse{
		HasConflict:  result.HasConflict,
		ReadRepaired: result.ReadRepaired,
	}
	for _, sib := range result.Siblings {
		resp.Siblings = append(resp.Siblings, node.WireValue{
			Value:    sib.Value,
			Clock:    sib.Clock,
			NodeID:   sib.NodeID,
			WallNano: sib.Timestamp.UnixNano(),
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// ── Core quorum logic ─────────────────────────────────────────────────────

const (
	GetTimeoutInSecs = 60 * time.Second
)

// Put writes a value for key across W nodes.
//
// Algorithm:
//  1. Pre-read: gather latest clock from all nodes via /internal/versions
//  2. Build new versioned value: merge all clocks + client BaseClock, increment ours
//  3. Fan out via POST /internal/replicate to all N nodes concurrently
//  4. Wait for W ACKs — return success. Remaining goroutines finish in background.
func (c *Coordinator) Put(ctx context.Context, key string, value []byte, baseClock clock.VectorClock) (WriteResult, error) {
	c.logger.Printf("calling PUT key=%q, value=%s", key, value)

	// Step 1: pre-read to get the latest known clock for this key.
	// Without this, two coordinators writing concurrently would both start
	// from clock.New() and create a false conflict even on sequential logical writes.
	latestClock := c.gatherLatestClock(ctx, key)

	// Step 2: build the new clock.
	newClock := clock.New()
	if baseClock != nil {
		newClock = newClock.Merge(baseClock)
	}

	newClock = newClock.Merge(latestClock)
	newClock = newClock.Increment(c.id)

	c.logger.Printf("PUT key=%q clock=%s", key, newClock)

	// vv is the version that coordinator writes to all node
	vv := storage.VersionedValue{
		Value:     value,
		Clock:     newClock,
		NodeID:    c.id,
		Timestamp: time.Now(),
	}

	// Step 2: fan-out write to all nodes, wait for W ACKs.
	ackCh := make(chan ackMsg, len(c.nodes))

	for _, nd := range c.nodes {
		go func(n NodeClient) {
			err := n.replicate(ctx, key, vv)
			if err != nil {
				c.logger.Printf("PUT key=%q → node=%s FAILED: %v", key, n.ID, err)
			} else {
				c.logger.Printf("PUT key=%q → node=%s OK", key, n.ID)
			}
			ackCh <- ackMsg{err}
		}(nd)
	}

	acks, errs := c.waitForQuorum(ctx, ackCh, c.quorum.N, c.quorum.W)
	if errs != nil {
		return WriteResult{}, fmt.Errorf("put failed: %v", errors.Join(errs...))
	}

	if acks < c.quorum.W {
		return WriteResult{}, fmt.Errorf("write quorum not reached: got %d/%d acks, errors: %v",
			acks, c.quorum.W, errs)
	}

	return WriteResult{
		Clock:  baseClock,
		Acks:   acks,
		NodeID: c.id,
	}, nil
}

// Get reads key from R nodes, detects conflicts, and applies read repair.
//
// Algorithm:
//  1. Fan out GET /internal/versions to all N nodes concurrently
//     and collect R responses, merge all returned siblings
//  2. Prune dominated versions — survivors are true concurrent siblings
//  3. If conflict: call reconcileFn, write back via replicate (read repair)
func (c *Coordinator) Get(ctx context.Context, key string) (ReadResult, error) {
	c.logger.Printf("calling GET key=%q", key)

	var allVersions []storage.VersionedValue
	var mu sync.Mutex

	// Step 1: fan-out reads to all nodes, wait for R ACKs.
	ackCh := make(chan ackMsg, len(c.nodes))

	ctxWT, cancel := context.WithTimeout(ctx, GetTimeoutInSecs)
	defer cancel()

	// Fan oout reads to all nodes
	for _, nd := range c.nodes {
		go func(n NodeClient) {
			result, err := n.versions(ctx, key)
			if err != nil {
				c.logger.Printf("GET key=%q → node=%s FAILED: %v", key, n.ID, err)
			} else {
				c.logger.Printf("GET key=%q → node=%s OK", key, n.ID)
				mu.Lock()
				allVersions = append(allVersions, result...)
				mu.Unlock()
			}
			ackCh <- ackMsg{err}
		}(nd)
	}

	// Wait for R acks
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
		c.logger.Printf("GET key=%q has CONFLICT (%d siblings) — attempting read repair on %+v",
			key, len(pruned), pruned)

		resolved := c.reconcile(key, pruned)

		// ReturnAll returns a zero VersionedValue — skip write-back.
		if resolved.Clock != nil {
			// Validate: resolved clock must dominate ALL siblings.
			for _, sib := range pruned {
				if !sib.IsDominatedBy(resolved) {
					// Skip repair
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
		go func(n NodeClient) {
			defer wg.Done()

			vvs, err := n.versions(ctx, key)
			if err != nil || len(vvs) == 0 {
				return
			}

			local := clock.New()
			for _, vv := range vvs {
				local = local.Merge(vv.Clock)
			}

			mu.Lock()
			merged = merged.Merge(local)
			mu.Unlock()
		}(nd)
	}

	wg.Wait()
	return merged
}

// writeBack replicates a reconciled version to all nodes (best-effort).
func (c *Coordinator) writeBack(ctx context.Context, key string, resolved storage.VersionedValue) {
	for _, nd := range c.nodes {
		go func(n NodeClient) {
			select {
			case <-ctx.Done():
				c.logger.Printf("READ REPAIR key=%q → node=%s CANCELLED", key, n.ID)
				return
			default:
			}

			if err := n.replicate(ctx, key, resolved); err != nil {
				c.logger.Printf("READ REPAIR key=%q → node=%s FAILED: %v", key, n.ID, err)
			} else {
				c.logger.Printf("READ REPAIR key=%q → node=%s OK", key, n.ID)
			}
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
func (c *Coordinator) waitForQuorum(ctx context.Context, ackCh <-chan ackMsg, total, need int) (int, []error) {
	acks, received := 0, 0
	var errs []error
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
