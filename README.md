# Vector Clock

A vector clock implementation for tracking causal ordering in distributed systems. Each clock is a list of (node, counter) pairs, allowing the system to determine if updates are causally related or concurrent.

Based on the approach described in Amazon's Dynamo paper: ["Dynamo: Amazon's Highly Available Key-value Store"](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf) (DeCandia et al., 2007).

## How it works

When a client reads an object, it receives the vector clock associated with that version. On subsequent writes, the client passes this clock back. The system uses it to track version history and detect conflicts.

## Handling conflicts

**Automatic (Syntactic) reconciliation** — Most of the time, new versions make old ones obsolete. The system can figure out which version is the right one.

**Manual (Semantic) reconciliation** — Sometimes there are multiple conflicting versions that can't be resolved automatically. In these cases, the client needs to decide how to merge them.