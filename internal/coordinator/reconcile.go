package coordinator

import "github.com/harshjoeyit/vectorclock/internal/storage"

// Pluggable reconciliation
type ReconcileFunc func(siblings []storage.VersionedValue) storage.VersionedValue

// Built-in strategies
// - LastWriteWins(siblings)   → uses wall clock as tiebreak
// - UnionMerge(siblings)      → for set-type values (like cart)
// - ReturnAll(siblings)       → push decision to client (true Dynamo style)
