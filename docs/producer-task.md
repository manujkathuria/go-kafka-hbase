# Implement the timed Kafka producer

Work item: [issue #3](https://github.com/manujkathuria/go-kafka-hbase/issues/3). Continue branch `codex/timed-kafka-producer` and its linked draft PR.

## Goal

Implement only this workload tool:

```text
Generate synthetic message → Publish to Kafka
```

Run at maximum speed for a configured generation duration, then finish outstanding writes. This is test tooling for the later processor, not a separate platform.

## Start here

Read [AGENTS.md](../AGENTS.md) and the [agreed producer scope](architecture.md#agreed-first-implementation-slice). Apply the standards/skills they route to, especially Kafka delivery, data ownership, service lifecycle, and meaningful testing. Existing source-project examples do not prescribe this application's structure.

## Implementation

- Start with one Go module and a small `cmd/producer` command. Keep implementation packages minimal; no service framework or speculative layers.
- Inputs: brokers, existing topic, and duration. Validate before publishing. Example interface: `producer -brokers localhost:9092 -topic messages.input -duration 10s`.
- Generate a fresh run ID and monotonic sequence numbers with deterministic synthetic content. Choose one fixed record size and a simple documented key mapping; keep these as named constants rather than new CLI options. This fixture format does not settle the HBase row schema or conversation-ordering contract.
- Use franz-go at a pinned compatible version. A straightforward loop of bounded, reusable batches with synchronous batch completion is a good starting point. No per-record goroutines or custom worker pool. Use client backpressure and bounded buffers.
- Preserve key/value storage until client completion. Check every result, including partially successful batches. Use idempotence and all-ISR acknowledgements; no Kafka transactions are required for this producer.
- Check the deadline while generating/enqueueing and configure bounded publication waits. An already submitted batch can finish during drain; do not promise process exit exactly at the duration deadline. Use a fresh bounded cleanup context after generation cancellation; resolve outstanding client ownership before buffer reuse/exit.
- On Ctrl+C, stop generating and attempt the same bounded drain. On publication failure, stop issuing new work, report observed outcomes, clean up, and exit non-zero. An unconfirmed outcome is not proof the record is absent from Kafka; do not blindly resend an entire batch.
- Print the run ID, submitted count, acknowledged count, failed/unconfirmed count, generation time, drain time, and acknowledged throughput with its timing boundary. Report average throughput over generation plus drain, not requested or guessed capacity.

Do not install, reconfigure, or stop Kafka or introduce a Docker stack as part of this task. Use an explicitly supplied development broker; report a missing prerequisite. Record broker settings and replication factor when reporting verification.

## Verification

1. Run build, vet, and meaningful unit/race checks for configuration, record encoding, and lifecycle behaviour. Use Go's testing package; do not add a large test framework.
2. Add an opt-in real-Kafka test using an isolated topic/run. Read back records and compare IDs, key mapping, and full payloads with independent expectations; producer counters alone do not prove storage correctness.
3. Exercise normal completion, interruption, and broker/publication failure with bounded waits. Do not use timing sleeps as proof of delivery.
4. Show a short successful run. A 60-second run is optional, not required to establish a throughput headline. Keep benchmark claims separate from race-instrumented tests.
5. Update the README with actual Go/client versions, prerequisites, commands, record format, key mapping, and verification performed. State clearly that HBase processing is not implemented.

## Delivery

Continue the issue, branch, and draft PR supplied with this document. Inspect local changes before working; stage only task changes. Commit verified increments to the same branch and update the PR description with actual outcomes. Mark it ready only after implementation and checks are complete. Do not merge unless the user authorizes it. No performance target is required for this workload generator.
