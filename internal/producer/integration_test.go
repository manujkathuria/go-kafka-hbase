package producer_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/manujkathuria/go-kafka-hbase/internal/producer"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TestRealKafkaProducerReadback is an opt-in integration test that talks to a real Kafka broker.
// Run with:
//
//	TEST_KAFKA_BROKERS="localhost:9092" TEST_KAFKA_TOPIC="messages.input" go test -v -run TestRealKafkaProducerReadback ./internal/producer
func TestRealKafkaProducerReadback(t *testing.T) {
	brokersEnv := os.Getenv("TEST_KAFKA_BROKERS")
	topicEnv := os.Getenv("TEST_KAFKA_TOPIC")
	if brokersEnv == "" || topicEnv == "" {
		t.Skip("skipping real Kafka integration test; set TEST_KAFKA_BROKERS and TEST_KAFKA_TOPIC to run")
	}

	brokers := strings.Split(brokersEnv, ",")
	for i := range brokers {
		brokers[i] = strings.TrimSpace(brokers[i])
	}

	// 1. Setup client for produce
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(topicEnv),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchCompression(kgo.Lz4Compression()),
	)
	if err != nil {
		t.Fatalf("failed to initialize franz-go producer client: %v", err)
	}
	defer client.Close()

	ctxPing, cancelPing := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelPing()
	if err := client.Ping(ctxPing); err != nil {
		t.Fatalf("cannot ping broker %v: %v", brokers, err)
	}

	// 2. Configure producer with a unique run ID and short duration
	cfg := producer.Config{
		Brokers:      brokers,
		Topic:        topicEnv,
		Duration:     2 * time.Second,
		DrainTimeout: 5 * time.Second,
		BatchSize:    100,
	}

	prod, err := producer.New(cfg, client, nil)
	if err != nil {
		t.Fatalf("failed to create producer: %v", err)
	}

	summary, err := prod.Run(context.Background())
	if err != nil {
		t.Fatalf("producer run failed: %v", err)
	}

	t.Logf("Producer completed: Submitted=%d, Acked=%d, Failed=%d in %v",
		summary.Submitted, summary.Acknowledged, summary.Failed, summary.TotalElapsed)

	if summary.Acknowledged == 0 {
		t.Fatalf("expected at least 1 acknowledged record")
	}

	// 3. Query partition end offsets AFTER producer finishes to establish a bounded topic range
	consumerClient, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topicEnv),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("failed to create consumer client: %v", err)
	}
	defer consumerClient.Close()

	admClient := kadm.NewClient(consumerClient)
	endOffsetsCtx, cancelEndOffsets := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelEndOffsets()
	listedOffsets, err := admClient.ListEndOffsets(endOffsetsCtx, topicEnv)
	if err != nil {
		t.Fatalf("failed to list partition end offsets: %v", err)
	}

	topicEndOffsets := make(map[int32]int64)
	listedOffsets.Each(func(lo kadm.ListedOffset) {
		if lo.Topic == topicEnv && lo.Err == nil {
			topicEndOffsets[lo.Partition] = lo.Offset
		}
	})
	if len(topicEndOffsets) == 0 {
		t.Fatalf("no partition end offsets found for topic %s", topicEnv)
	}

	t.Logf("Bounded end offsets for topic %s: %v", topicEnv, topicEndOffsets)

	// 4. Consume the topic up to end offsets
	// We verify that:
	// - Every record for this run matches independent expectations (no shared helper).
	// - No duplicates or unexpected sequence numbers exist.
	// - Exact total count of read records for this run equals summary.Acknowledged.
	readCtx, cancelRead := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancelRead()

	// Track partitions that have reached end offsets
	currentOffsets := make(map[int32]int64)
	partitionsDone := func() bool {
		for p, endOffset := range topicEndOffsets {
			if currentOffsets[p] < endOffset {
				return false
			}
		}
		return true
	}

	var totalReadForRun uint64
	seenSeqs := make(map[uint64]struct{})

	for !partitionsDone() {
		fetches := consumerClient.PollFetches(readCtx)
		if errs := fetches.Errors(); len(errs) > 0 {
			t.Fatalf("fetch errors: %v", errs)
		}

		iter := fetches.RecordIter()
		for !iter.Done() {
			rec := iter.Next()
			// Update offset progress for this partition
			if rec.Offset+1 > currentOffsets[rec.Partition] {
				currentOffsets[rec.Partition] = rec.Offset + 1
			}

			// Validate minimum wire length (1 byte magic + 16 bytes runID + 8 bytes seq + 2 bytes len + 128 payload = 155 bytes)
			const expectedTotalWireSize = 1 + 16 + 8 + 2 + 128
			if len(rec.Value) != expectedTotalWireSize {
				continue
			}
			if rec.Value[0] != 0x01 { // Magic byte
				continue
			}

			// Extract RunID directly from wire bytes
			var recRunID [16]byte
			copy(recRunID[:], rec.Value[1:17])
			if recRunID != summary.RunID {
				// Record belongs to another run or test
				continue
			}

			totalReadForRun++

			// Extract SeqNum and ValidLen directly without production decoder
			recSeq := binary.BigEndian.Uint64(rec.Value[17:25])
			recLen := binary.BigEndian.Uint16(rec.Value[25:27])

			if recLen != 128 {
				t.Fatalf("record for seq %d has unexpected ValidLen %d (want 128)", recSeq, recLen)
			}

			// Reject duplicate sequence numbers
			if _, exists := seenSeqs[recSeq]; exists {
				t.Fatalf("duplicate record detected for sequence number %d in run %x", recSeq, summary.RunID)
			}
			seenSeqs[recSeq] = struct{}{}

			// Verify sequence is within expected bounds [0, summary.Acknowledged)
			if recSeq >= summary.Acknowledged {
				t.Fatalf("unexpected sequence number %d (>= acknowledged count %d)", recSeq, summary.Acknowledged)
			}

			// Independent Key check:
			// Expected key is format "key-%02d" where bucket = seq % 16
			expectedKey := []byte(fmt.Sprintf("key-%02d", recSeq%16))
			if !bytes.Equal(rec.Key, expectedKey) {
				t.Errorf("key mismatch for seq %d: got %s, want %s", recSeq, string(rec.Key), string(expectedKey))
			}

			// Independent Payload check:
			// Prefix: "RUN:<hex-prefix>:SEQ:<seq>:"
			// Padding: byte((i + seq) & 0xff) up to 128 bytes
			expectedPrefix := fmt.Sprintf("RUN:%x:SEQ:%d:", summary.RunID[:4], recSeq)
			expectedPayload := make([]byte, 128)
			n := copy(expectedPayload, expectedPrefix)
			for i := n; i < 128; i++ {
				expectedPayload[i] = byte((uint64(i) + recSeq) & 0xff)
			}

			actualPayload := rec.Value[27:155]
			if !bytes.Equal(actualPayload, expectedPayload) {
				t.Fatalf("payload mismatch for seq %d\ngot:  %x\nwant: %x", recSeq, actualPayload, expectedPayload)
			}
		}

		if readCtx.Err() != nil {
			t.Fatalf("read context timeout while consuming topic up to end offsets: %v", readCtx.Err())
		}
	}

	// 5. Assert exact record count
	if totalReadForRun != summary.Acknowledged {
		t.Fatalf("total read records for run %x = %d, expected exactly %d",
			summary.RunID, totalReadForRun, summary.Acknowledged)
	}

	// 6. Assert complete contiguous sequence coverage
	for seq := uint64(0); seq < summary.Acknowledged; seq++ {
		if _, ok := seenSeqs[seq]; !ok {
			t.Errorf("missing expected sequence number %d in readback", seq)
		}
	}
}
