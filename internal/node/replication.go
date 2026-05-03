package node

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/harshjoeyit/vectorclock/internal/storage"
)

// handleGet handles client read requests.
// Returns all siblings — if HasConflict is true the client must reconcile.
func (n *Node) handleReplicate(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}

	var wv wireValue
	if err := json.NewDecoder(r.Body).Decode(&wv); err != nil {
		http.Error(w, "bad request body", http.StatusBadRequest)
		return
	}
	wv.toVersionedValue()
	result := n.store.Put(key, wv.toVersionedValue())
	n.logger.Printf("REPLICATED key=%q result=%s clock=%s", key, result, wv.Clock)

	w.WriteHeader(http.StatusNoContent)
}

// --- Replication ---------------------------------------------------------

// replicate fans out a write to all known peers.
// Each peer gets its own goroutine — failures are isolated.
// Network conditions (latency, drop rate) are applied per peer.
func (n *Node) replicate(key string, vv storage.VersionedValue) {
	n.mu.RLock()
	peers := make([]Peer, len(n.peers))
	copy(peers, n.peers)
	n.mu.RUnlock()

	// Convert VersionedValue to wireValue
	body, _ := json.Marshal(toWireValue(vv))

	for _, peer := range n.peers {
		go func(p Peer) {
			// Simulate network latency
			latency := n.simulateLatency()
			time.Sleep(latency)

			// Simulate packet loss / partition
			if n.shouldDrop() {
				n.logger.Printf("REPLICATE key=%q → %s DROPPED (simulated)", key, p.ID)
				return
			}

			url := fmt.Sprintf("%s/internal/replicate/%s", p.Addr, key)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			resp, err := http.DefaultClient.Do(mustRequestWithCtx(ctx, http.MethodPost, url, body))
			if err != nil {
				n.logger.Printf("REPLICATE key=%q → %s FAILED: %v", key, p.ID, err)
				return
			}
			resp.Body.Close()
			n.logger.Printf("REPLICATE key=%q → %s OK latency=%s", key, p.ID, latency)
		}(peer)
	}
}
