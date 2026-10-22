# Agent guide

## Project and current scope

Build a Go showcase for reliable, high-performance Kafka-to-HBase persistence. Read [architecture](docs/architecture.md) before design or implementation decisions; it distinguishes agreed requirements from proposals and open questions. Current status is documentation and guidance only. The agreed first code slice is a basic timed maximum-speed Kafka producer, described in the architecture document. Discussion does not authorize application implementation.

The synthetic producer supplies workload; unreliable source transports and redundant ingestion are out of scope. Do not copy stream-eos application architecture, fixed partition ownership, FIFO deduplication, or Kafka-only transactional guarantees into this project without a reasoned design decision.

## Essential invariants

- Preserve data integrity before optimizing speed. Bound memory, queues, in-flight writes, and retries; use backpressure rather than silent drops.
- Never advance durable Kafka progress beyond any record whose required HBase persistence is unconfirmed. With concurrent completion, track the completed prefix of consumed input per partition rather than the greatest completed offset. Kafka offsets need not be numerically contiguous.
- Kafka and HBase do not share a Kafka transaction. A successful HBase write followed by a crash before recording Kafka progress can cause replay; the row/write contract must make replay safe.
- Unknown write outcomes are not success or proof of failure. Preserve retry/recovery possibilities and verify actual stored results.
- No performance, delivery, or failover guarantee is established until verified under stated conditions. Single-node dependency tests do not prove cluster failover.

## Select guidance before relevant decisions

Read the [pair-programming agreement](.agents/standards/pair-programming.md) at engineering task start. Use the [standards index](.agents/standards/engineering-standards.md) to load applicable standards; reuse unchanged guidance still available in context.

Identify the behaviour affected and the matching guidance before implementation. Selection is cumulative and not based only on filenames. Re-evaluate when scope expands, and reload relevant guidance after context loss. Read the actual documents, not just this routing table.

| Work involving | Required skill |
| --- | --- |
| Delivery boundaries, acknowledgements, ordering, backpressure, cancellation, recovery | [Correct delivery](.agents/skills/correct-delivery/SKILL.md) |
| Kafka producers/consumers, partitioning, offset progress, rebalances, transactions, broker tests | [Go Kafka](.agents/skills/go-kafka/SKILL.md) |
| Typed records, slices, buffers, batches, codecs, asynchronous handoffs | [Data ownership](.agents/skills/go-data-ownership/SKILL.md) |
| Value/pointer choices, storage lifetimes, reuse, allocation diagnosis | [Stack and heap](.agents/skills/go-stack-vs-heap/SKILL.md) |
| Repeatedly stored structs or memory footprint | [Struct alignment](.agents/skills/go-struct-alignment/SKILL.md) |
| Network/file loops, database writes, batching, I/O bottlenecks | [I/O batching](.agents/skills/go-io-batching/SKILL.md) |
| Go behaviour changes, bug fixes, meaningful unit tests | [Unit testing](.agents/skills/go-unit-testing/SKILL.md) |
| Profiles, benchmarks, load tests, resource tuning, performance claims | [Profiling and benchmarks](.agents/skills/go-profiling-benchmarks/SKILL.md) |

No HBase-specific skill exists yet. Consult the official documentation and source for the selected server/client versions before establishing write durability, retry, versioning, atomicity, or connection-lifecycle semantics. A new skill is justified only by a recurring workflow.

## Collaboration and delivery

Follow the [development workflow](.agents/standards/development-workflow.md) for issues, branches, checks, commits, and PRs within the user's authorization. The GitHub repository is https://github.com/manujkathuria/go-kafka-hbase. Track meaningful work in issues and deliver it through branches and PRs; do not invent issue IDs. This initial publication contains documentation only.

Explain design through concrete data journeys, ownership, and failure behaviour. Develop with AI as a pair programmer while helping the user understand and defend the implementation. Material changes to scope, guarantees, or architecture require discussion; routine details within an agreed implementation slice do not.

Shared standards and the seven Go skills were copied from stream-eos at commit a58aeb3f1910cecff6165e01528e6a011afafd23. The Kafka skill was adapted to remove source-project A/B ingestion assumptions and to distinguish Kafka-only transactions from external database writes. Correct-delivery guidance was copied from the interview workspace. Keep copied content and local project decisions distinct; no automatic synchronization is implied. Maintain guidance when confirmed facts change. No YAML catalogue or custom CLI is required.
