---
name: correct-delivery
description: Design or review delivery paths in this learning workspace, including IPC, feeds, queues, and streaming services. Use when changing message ownership, ordering, buffering, acknowledgements, or recovery.
---

# Correct delivery first

Correct delivery is the requirement; performance is optimized within that requirement.

Treat this as the base principle for this workspace. Allocation and latency targets
must not override required behavior.

Before implementation, define send success, ordering scope, payload ownership,
bounded capacity, backpressure, cancellation, shutdown, and recovery guarantees.
Never silently overwrite unread data, drop messages, or truncate payloads.
Return explicit errors for oversized payloads and corruption.

Keep unresolved guarantees explicit. mmap and gRPC alone do not provide crash-safe
delivery. Durable recovery needs persistence, acknowledgements, replay, stable
message identity, and duplicate handling. Do not silently expand a learning task
into implementing durability before the requirements are settled.

For mmap, publish only complete frames with correctly aligned synchronization.
Validate mapping version, dimensions, and ownership before use. Go race tests
cannot prove cross-process shared-memory safety.

For gRPC, use a persistent stream when appropriate. Send success is not remote
receipt or processing. Do not blindly retry ambiguous failures unless the
protocol handles duplicates.

Verify payload equality, ordering, full buffers, small receive buffers,
cancellation, shutdown, and any promised recovery behavior before benchmarking.
Exercise IPC implementations in separate processes.

Compare equivalent delivery milestones with equal payloads and comparable
process placement. Report tail latency, allocations, and polling CPU costs where
relevant. Optimize copies and syscalls only while preserving the contract.
