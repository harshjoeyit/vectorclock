package storage

import (
	"fmt"
	"slices"
	"sync"
)

type PutResult int

const (
	// PutAccepted means the version was stored (new or updated).
	PutAccepted PutResult = iota

	// PutConflict means the incoming version is concurrent with
	// existing version(s) - both are retained as siblings
	PutConflict

	// PutStale means the incoming version is dominated by what's
	// already stored, hence incoming version is discarded
	PutStale
)

func (r PutResult) String() string {
	switch r {
	case PutAccepted:
		return "Accepted"
	case PutConflict:
		return "Conflict"
	case PutStale:
		return "Stale"
	default:
		return "Unknown"
	}
}

// GetResult is returned on every read
type GetResult struct {
	Siblings    []VersionedValue // len>1 means unreconciled conflict
	Found       bool
	HasConflict bool
}

// Store is a single node's in-memory versioned KV store.
//
// Design decisions:
//   - One RWMutex per Store (not per key) — acceptable for a prototype;
//     production would use striped locks or per-key sync.
//   - Siblings are never silently merged — that is the coordinator's job.
//   - Put is idempotent for Equal clocks (replayed replication is safe).
type Store struct {
	nodeID string
	data   map[string][]VersionedValue // key -> siblings
	mu     sync.RWMutex
}

func New(nodeID string) *Store {
	return &Store{
		nodeID: nodeID,
		data:   make(map[string][]VersionedValue),
	}
}

// NodeID returns the node this store belongs to.
func (s *Store) NodeID() string { return s.nodeID }

// Put writes a new version for key:
// Note: Put does not reconcile, it's done by coordinator on read.
//
//  1. If incoming is dominated by ANY sibling → discard (PutStale).
//  2. Remove any existing siblings that are dominated by the incoming clock.
//  3. Add the incoming version to the surviving siblings.
//  4. If surviving siblings > 1 after the write → PutConflict, else PutAccepted
func (s *Store) Put(key string, incoming VersionedValue) PutResult {
	s.mu.Lock()
	defer s.mu.Unlock()

	existing := s.data[key]

	// First write. Key does not exist
	if len(existing) == 0 {
		s.data[key] = []VersionedValue{incoming}
		return PutAccepted
	}

	// Step 1: Is the incoming version already stale?
	for _, sib := range existing {
		if incoming.IsDominatedBy(sib) {
			// sib dominates or equals incoming → discard
			return PutStale
		}
	}

	// Step 2: Prune siblings that are now dominated by the incoming version.
	surviving := existing[:0] // reuse underlying array
	for _, sib := range existing {
		if !sib.IsDominatedBy(incoming) {
			surviving = append(surviving, sib)
		}
	}

	// Step 3: Add incoming to survivors.
	surviving = append(surviving, incoming)
	s.data[key] = surviving

	// Step 4: Report conflict if we still have multiple siblings.
	if len(surviving) > 1 {
		return PutConflict
	}
	return PutAccepted
}

// Get returns all current versions for key.
// len(result.Siblings) > 1 means there is an unreconciled conflict.
func (s *Store) Get(key string) GetResult {
	s.mu.Lock()
	defer s.mu.Unlock()

	siblings, ok := s.data[key]
	if !ok || len(siblings) == 0 {
		return GetResult{Found: false}
	}

	// Return defensive copies — callers must not mutate store state.
	copiedSiblings := make([]VersionedValue, len(siblings))
	for i, sib := range siblings {
		copiedSiblings[i] = VersionedValue{
			Value:     slices.Clone(sib.Value),
			Clock:     sib.Clock.Clone(),
			NodeID:    sib.NodeID,
			Timestamp: sib.Timestamp,
		}
	}

	return GetResult{
		Siblings:    copiedSiblings,
		Found:       true,
		HasConflict: len(siblings) > 1,
	}
}

// Reconcile replaces all siblings for key with a single reconciled version.
// Called by the coordinator after client resolves a conflict.
func (s *Store) Reconcile(key string, resolved VersionedValue) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = []VersionedValue{resolved}
}

// Keys returns all keys currently stored. Useful for debugging/inspection.
func (s *Store) Keys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	return keys
}

// DebugDump prints the full store state — siblings, clocks, values.
func (s *Store) DebugDump() string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(s.data) == 0 {
		return fmt.Sprintf("[%s] <empty store>\n", s.nodeID)
	}

	out := fmt.Sprintf("[%s] store dump:\n", s.nodeID)
	for key, siblings := range s.data {
		if len(siblings) == 1 {
			out += fmt.Sprintf("  key=%q  clock=%s  value=%s\n",
				key, siblings[0].Clock, siblings[0].Value)
		} else {
			out += fmt.Sprintf("  key=%q  *** %d SIBLINGS (CONFLICT) ***\n", key, len(siblings))
			for i, sib := range siblings {
				out += fmt.Sprintf("    [%d] clock=%s  value=%s  from=%s\n",
					i, sib.Clock, sib.Value, sib.NodeID)
			}
		}
	}
	return out
}
