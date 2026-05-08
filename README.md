# rafty

A small from-scratch [Raft](https://raft.github.io/) implementation in Rust, with
a swappable RPC layer and a fault injector for testing.

## Features 

### Done

- Leader election with randomized timeouts and step-down on a higher term seen.
- PreVote to suppress disruptive elections by returning isolated nodes
  (no `current_term` bump until a quorum confirms it would actually grant a real vote).
- Log replication driven by per-peer `next_index` / `match_index`.
- `AppendEntries` consistency check (`prev_log_index` + `prev_log_term`),
  truncate-on-conflict, and `leader_commit` propagation.
- Majority commit advancement honoring the Figure 8 current-term rule.
- Vote rejection when the candidate's log is not at least as up-to-date as ours.
- A simple `key=value` state machine that every node converges on.
- Pluggable transport via the `RPC` trait.
- A fault injector (`FaultyRPC`) that wraps any transport.
- Structured logs through `tracing` with a per-node span.

### Todo 

- Persistence of `(current_term, voted_for, log)` — purely in-memory today.
- Snapshotting / log compaction.
- Cluster membership changes.
- A non-memory transport (the `RPC` trait is the seam; a `TcpRPC` drops in).
- Linearizable reads (ReadIndex / leader leases).
- Batched and pipelined `AppendEntries`.

## Running

```bash
cargo run
```

The default demo spins up a 3-node cluster, feeds it one `kN=vN` command per
second from a client thread, and runs a fault injector that cycles through
several patterns. Watch the `applied` events to see all three nodes converge on
the same `kv_size`.

Set `RUST_LOG` to filter, e.g. `RUST_LOG=debug cargo run` to see vote denials,
client appends, and back-off detail.
