package producer_test

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/manujkathuria/go-kafka-hbase/internal/producer"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TestRealKafkaProducerReadback is an opt-in integration test that talks to a real Kafka broker.
// Run with:
//   TEST_KAFKA_BROKERS="localhost:9092" TEST_KAFKA_TOPIC="messages.input" go test -v -run TestRealKafkaProducerReadback ./internal/producer
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

	// 1. Setup client
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(topicEnv),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchCompression(kgo.Lz4Compression()),
		kgo.ConsumeTopics(topicEnv),
	)
	if err != nil {
		t.Fatalf("failed to initialize franz-go client: %v", err)
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

	// 3. Independent Readback Verification:
	// Consume from the topic and verify that all records for summary.RunID match expectations.
	consumerClient, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topicEnv),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("failed to create consumer client: %v", err)
	}
	defer consumerClient.Close()

	readCtx, cancelRead := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancelRead()

	consumedThisRun := make(map[uint64]producer.SyntheticRecord)

	for uint64(len(consumedThisRun)) < summary.Acknowledged {
		fetches := consumerClient.PollFetches(readCtx)
		if errs := fetches.Errors(); len(errs) > 0 {
			t.Fatalf("fetch errors: %v", errs)
		}

		iter := fetches.RecordIter()
		for !iter.Done() {
			rec := iter.Next()
			if len(rec.Value) < producer.RecordHeaderSize+producer.FixedPayloadSize {
				continue
			}
			if rec.Value[0] != producer.RecordMagic {
				continue
			}

			var synRec producer.SyntheticRecord
			if err := synRec.Decode(rec.Value); err != nil {
				continue
			}

			if synRec.RunID == summary.RunID {
				// Verify key mapping
				expectedKey := producer.KeyForSeq(synRec.SeqNum)
				if !bytes.Equal(rec.Key, expectedKey) {
					t.Errorf("key mismatch for seq %d: got %s, want %s", synRec.SeqNum, string(rec.Key), string(expectedKey))
				}

				// Verify payload content independently
				var expectedRec producer.SyntheticRecord
				expectedRec.FillDeterministicPayload(summary.RunID, synRec.SeqNum)
				if !bytes.Equal(synRec.Payload[:], expectedRec.Payload[:]) {
					t.Errorf("payload mismatch for seq %d", synRec.SeqNum)
				}

				consumedThisRun[synRec.SeqNum] = synRec
			}
		}

		if readCtx.Err() != nil {
			break
		}
	}

	if uint64(len(consumedThisRun)) != summary.Acknowledged {
		t.Fatalf("readback count mismatch: got %d records for run %x, want %d",
			len(consumedThisRun), summary.RunID, summary.Acknowledged)
	}

	// Verify monotonic contiguous sequences [0, summary.Acknowledged)
	for seq := uint64(0); seq < summary.Acknowledged; seq++ {
		if _, ok := consumedThisRun[seq]; !ok {
			t.Errorf("missing expected seq %d in consumed records", seq)
		}
	}
}
