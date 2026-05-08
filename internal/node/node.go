// Package node implements a single distributed node.
//
// Each node is an HTTP server that:
//   - Accepts client writes (PUT /keys/{key})
//   - Accepts client reads  (GET /keys/{key})
//   - Accepts replicated writes from peers (POST /internal/replicate/{key})
//   - Fans out writes asynchronously to all known peers
//
// Network behaviour is configurable via NodeConfig to simulate
// real-world conditions: latency, packet loss, node failures.
package node

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/harshjoeyit/vectorclock/internal/clock"
	"github.com/harshjoeyit/vectorclock/internal/storage"
)

// NodeConfig controls a node's network behaviour.
// Tweak these to simulate different failure scenarios.
type NodeConfig struct {
	// Addr: the address this node listens on, e.g. "http://localhost:8082"
	addr string

	// MinLatency / MaxLatency: simulated per-request network delay.
	MinLatency time.Duration
	MaxLatency time.Duration

	// DropRate: probability [0.0, 1.0] that an outbound replication
	// message is silently dropped. Simulates packet loss / partition.
	DropRate float64
}

func (n NodeConfig) Addr() string {
	return n.addr
}

func DefaultConfig(addr string) NodeConfig {
	return NodeConfig{
		addr:       addr,
		MinLatency: 5 * time.Millisecond,
		MaxLatency: 30 * time.Millisecond,
		DropRate:   0.0,
	}
}

// Peer is another node this node can replicate to.
type Peer struct {
	ID   string
	Addr string // e.g. "http://localhost:8082"
}

type Node struct {
	id     string
	config NodeConfig
	store  *storage.Store
	peers  []Peer
	mu     sync.RWMutex // protects peer list
	server *http.Server
	logger *log.Logger
}

func New(id string, config NodeConfig) *Node {
	return &Node{
		id:     id,
		peers:  make([]Peer, 0),
		config: config,
		store:  storage.New(id),
		logger: log.New(log.Writer(), fmt.Sprintf("[%s] ", id), log.LstdFlags|log.Lmsgprefix),
	}
}

// AddPeer registers a peer for replication
func (n *Node) AddPeer(p Peer) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.peers == nil {
		n.peers = make([]Peer, 0)
	}
	n.peers = append(n.peers, p)
}

// ID returns the node identifier.
func (n *Node) ID() string { return n.id }

// Store exposes the underlying store for coordinator access.
func (n *Node) Store() *storage.Store { return n.store }

func (n *Node) Config() NodeConfig { return n.config }

// --- Lifecycle -----------------------------------------------------------

// Start boots the node's HTTP server.
func (n *Node) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /keys/{key}", n.handlePut)
	mux.HandleFunc("GET /keys/{key}", n.handleGet)
	mux.HandleFunc("GET /internal/versions/{key}", n.handleGetVersions)
	mux.HandleFunc("POST /internal/replicate/{key}", n.handleReplicate)

	ln, err := net.Listen("tcp", n.config.addr)
	if err != nil {
		return fmt.Errorf("node %s: listen: %w", n.id, err)
	}

	n.server = &http.Server{
		Handler: mux,
	}

	go func() {
		if err := n.server.Serve(ln); err != nil && err != http.ErrServerClosed {
			n.logger.Printf("server error: %v", err)
		}
	}()

	n.logger.Printf("listening on %s", n.config.addr)
	return nil
}

// Stop gracefully shuts down the node.
func (n *Node) Stop(ctx context.Context) error {
	if n.server == nil {
		return nil
	}
	return n.server.Shutdown(ctx)
}

// --- HTTP Handlers -------------------------------------------------------

// WireValue is the JSON shape used on the wire.
// Keeping it explicit (not exposing storage internals) is intentional.
type WireValue struct {
	Value  []byte            `json:"value"`
	Clock  clock.VectorClock `json:"clock"`
	NodeID string            `json:"node_id"`
	// Unix nano — wall clock only for LWW tiebreak
	WallNano int64 `json:"wall_nano"`
}

// -- helper functions for conversion

func (wv WireValue) ToVersionedValue() storage.VersionedValue {
	return storage.VersionedValue{
		Value:     wv.Value,
		Clock:     wv.Clock,
		NodeID:    wv.NodeID,
		Timestamp: time.Unix(0, wv.WallNano),
	}
}

func toWireValue(vv storage.VersionedValue) WireValue {
	return WireValue{
		Value:    vv.Value,
		Clock:    vv.Clock,
		NodeID:   vv.NodeID,
		WallNano: vv.Timestamp.UnixNano(),
	}
}

type PutRequest struct {
	Value []byte `json:"value"`
	// Client echoes back the clock it last read (for causal consistency).
	// On first write this is nil/empty.
	BaseClock clock.VectorClock `json:"base_clock,omitempty"`
}

// PutResponse is returned to the client.
type PutResponse struct {
	NodeID string            `json:"node_id"`
	Clock  clock.VectorClock `json:"clock"`
	Result string            `json:"result"`
}

// GetResponse is returned to the client on read.
// If HasConflict is true, Siblings contains all concurrent versions —
// the client is responsible for reconciliation.
type GetResponse struct {
	HasConflict bool        `json:"has_conflict"`
	Siblings    []WireValue `json:"siblings"`
}

// handlePut handles client write requests.
//
// Flow:
//  1. Build a new clock: merge client's base clock with our local knowledge,
//     then increment our own counter.
//  2. Store locally.
//  3. Async-replicate to all peers.
func (n *Node) handlePut(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}

	var req PutRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request body", http.StatusBadRequest)
		return
	}

	// Merge client's clock
	newClock := clock.New()
	if req.BaseClock != nil {
		newClock = newClock.Merge(req.BaseClock)
	}

	// Get all the existing versions for this key and merge to newClock
	// (acknowledging that current write happens with knowledge of all existing versions)
	existing := n.store.Get(key)
	if existing.Found {
		for _, sib := range existing.Siblings {
			newClock = newClock.Merge(sib.Clock)
		}
	}

	// Increment clock for current node
	newClock = newClock.Increment(n.id)

	vv := storage.VersionedValue{
		Value:     req.Value,
		Clock:     newClock,
		NodeID:    n.id,
		Timestamp: time.Now(),
	}

	result := n.store.Put(key, vv)
	n.logger.Printf("PUT key=%q result=%s clock=%s", key, result, newClock)

	// Fan out to peers asynchronously without blocking the client
	go n.replicate(key, vv)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(PutResponse{
		NodeID: n.id,
		Clock:  newClock,
		Result: result.String(),
	})
}

// handleGet handles client read requests.
// Returns all siblings — if HasConflict is true the client must reconcile.
func (n *Node) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}

	result := n.store.Get(key)
	if !result.Found {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	resp := GetResponse{HasConflict: result.HasConflict}
	resp.Siblings = make([]WireValue, 0, len(result.Siblings)) // allocate space
	for _, sib := range result.Siblings {
		resp.Siblings = append(resp.Siblings, toWireValue(sib))
	}

	if result.HasConflict {
		n.logger.Printf("GET key=%q → %d SIBLINGS (conflict)", key, len(result.Siblings))
	} else {
		n.logger.Printf("GET key=%q → clock=%s", key, result.Siblings[0].Clock)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// handleGetVersions is the internal read endpoint used by the coordinator.
// Unlike handleGet (which is client-facing and may trigger reconciliation
// at a higher layer), this returns raw siblings with no interpretation.
// The coordinator collects these from R nodes and does its own pruning.
func (n *Node) handleGetVersions(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}

	result := n.store.Get(key)
	if !result.Found {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	var siblings []WireValue
	for _, sib := range result.Siblings {
		siblings = append(siblings, toWireValue(sib))
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(siblings)
}

// -- helper
func MustRequestWithCtx(ctx context.Context, method, url string, body []byte) *http.Request {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}

	// log.Printf("making HTTP %s request to: %s", method, url)

	req, err := http.NewRequestWithContext(ctx, method, url, reader)
	if err != nil {
		log.Fatalf("failed to make method: %s request to url: %s", method, url)
	}

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	return req
}

// --- Network simulation --------------------------------------------------

func (n *Node) simulateLatency() time.Duration {
	if n.config.MaxLatency == 0 {
		return 0
	}
	delta := n.config.MaxLatency - n.config.MinLatency
	//nolint:gosec // weak rand is fine for simulation
	jitter := time.Duration(rand.Int63n(int64(delta + 1)))
	return n.config.MinLatency + jitter
}

func (n *Node) shouldDrop() bool {
	if n.config.DropRate <= 0 {
		return false
	}
	//nolint:gosec
	return rand.Float64() < n.config.DropRate
}

// SetDropRate changes the drop rate at runtime — useful for simulating
// a partition starting and healing mid-test.
func (n *Node) SetDropRate(rate float64) {
	n.config.DropRate = rate
}

// DebugDump prints the node's store state.
func (n *Node) DebugDump() string {
	return n.store.DebugDump()
}
