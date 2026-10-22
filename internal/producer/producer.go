package producer

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Default constants for batching and bounded queues.
const (
	DefaultBatchSize    = 1000
	DefaultDrainTimeout = 10 * time.Second
)

// Config holds runtime parameters for the workload producer.
type Config struct {
	Brokers      []string
	Topic        string
	Duration     time.Duration
	DrainTimeout time.Duration
	BatchSize    int
	RunID        [16]byte // optional, generated if empty
}

// Validate checks that all required configuration parameters are valid.
func (c *Config) Validate() error {
	if len(c.Brokers) == 0 {
		return errors.New("config: at least one broker address is required")
	}
	for _, b := range c.Brokers {
		if strings.TrimSpace(b) == "" {
			return errors.New("config: broker address cannot be blank")
		}
	}
	if strings.TrimSpace(c.Topic) == "" {
		return errors.New("config: topic cannot be blank")
	}
	if c.Duration <= 0 {
		return errors.New("config: duration must be positive")
	}
	if c.BatchSize <= 0 {
		c.BatchSize = DefaultBatchSize
	}
	if c.DrainTimeout <= 0 {
		c.DrainTimeout = DefaultDrainTimeout
	}
	return nil
}

// Summary reports verified execution counters and timings.
type Summary struct {
	RunID        [16]byte
	Submitted    uint64
	Acknowledged uint64
	Failed       uint64
	GenElapsed   time.Duration
	DrainElapsed time.Duration
	TotalElapsed time.Duration
}

// Print formats the report to the given writer.
func (s *Summary) Print(w io.Writer) {
	var ackRate float64
	if s.TotalElapsed > 0 {
		ackRate = float64(s.Acknowledged) / s.TotalElapsed.Seconds()
	}

	fmt.Fprintf(w, "\n--- Producer Execution Summary ---\n")
	fmt.Fprintf(w, "Run ID:          %x\n", s.RunID)
	fmt.Fprintf(w, "Submitted:       %d\n", s.Submitted)
	fmt.Fprintf(w, "Acknowledged:    %d\n", s.Acknowledged)
	fmt.Fprintf(w, "Failed/Unacked:  %d\n", s.Failed)
	fmt.Fprintf(w, "Generation Time: %v\n", s.GenElapsed)
	fmt.Fprintf(w, "Drain Time:      %v\n", s.DrainElapsed)
	fmt.Fprintf(w, "Total Time:      %v\n", s.TotalElapsed)
	fmt.Fprintf(w, "Throughput (avg): %.2f records/sec (across %v total window)\n", ackRate, s.TotalElapsed)
	fmt.Fprintf(w, "----------------------------------\n\n")
}

// KafkaClient abstracts the kgo.Client produce methods needed by Producer.
// This allows testing orchestration without a real broker while production uses *kgo.Client.
type KafkaClient interface {
	Produce(ctx context.Context, r *kgo.Record, promise func(*kgo.Record, error))
	Flush(ctx context.Context) error
	Close()
}

// Producer drives bounded batch generation and publishes to Kafka.
type Producer struct {
	cfg    Config
	client KafkaClient
	logger *slog.Logger

	submitted atomic.Uint64
	acked     atomic.Uint64
	failed    atomic.Uint64
	closed    atomic.Bool
}

// New creates a new Producer with validated configuration.
func New(cfg Config, client KafkaClient, logger *slog.Logger) (*Producer, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	var zeroID [16]byte
	if cfg.RunID == zeroID {
		if _, err := rand.Read(cfg.RunID[:]); err != nil {
			return nil, fmt.Errorf("failed to generate run ID: %w", err)
		}
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Producer{
		cfg:    cfg,
		client: client,
		logger: logger,
	}, nil
}

// batchSlot holds the storage and completion tracking for an in-flight batch.
// Because the underlying Kafka client retains record byte references until the produce
// promise callback executes, a batchSlot's buffer must NOT be overwritten or reused
// until all in-flight callbacks have completed.
type batchSlot struct {
	buf     []byte
	records []kgo.Record
	wg      sync.WaitGroup
	active  bool
}

// Run executes the generation loop until ctx is canceled (or duration expires),
// then performs a bounded drain.
// Total shutdown and cleanup time is strictly bounded by DrainTimeout.
func (p *Producer) Run(ctx context.Context) (*Summary, error) {
	genCtx, genCancel := context.WithTimeout(ctx, p.cfg.Duration)
	defer genCancel()

	p.logger.Info("starting producer workload",
		"run_id", fmt.Sprintf("%x", p.cfg.RunID),
		"topic", p.cfg.Topic,
		"duration", p.cfg.Duration,
		"batch_size", p.cfg.BatchSize,
	)

	genStart := time.Now()
	var runErr error

	encodedRecordSize := RecordHeaderSize + FixedPayloadSize
	// Double buffering: 2 batch slots.
	// Slot i is populated and submitted. Before reusing slot i on a subsequent iteration,
	// we wait for slot i's previous completion. This bounds memory to 2 batches
	// while allowing safe pipelined generation and strict ownership isolation.
	slots := [2]batchSlot{
		{
			buf:     make([]byte, p.cfg.BatchSize*encodedRecordSize),
			records: make([]kgo.Record, p.cfg.BatchSize),
		},
		{
			buf:     make([]byte, p.cfg.BatchSize*encodedRecordSize),
			records: make([]kgo.Record, p.cfg.BatchSize),
		},
	}

	var seq uint64
	slotIdx := 0

	// Helper to wait for a slot's completion with context awareness
	waitSlot := func(ctx context.Context, slot *batchSlot) error {
		if !slot.active {
			return nil
		}
		done := make(chan struct{})
		go func() {
			slot.wg.Wait()
			close(done)
		}()
		select {
		case <-done:
			slot.active = false
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

loop:
	for {
		select {
		case <-genCtx.Done():
			break loop
		default:
		}

		slot := &slots[slotIdx]
		// If this slot was previously submitted, wait for its completion before reusing its buffer.
		// Crucial: this wait respects genCtx. If Kafka stalls or stops completing writes,
		// we exit generation and proceed to the bounded drain phase.
		if slot.active {
			if err := waitSlot(genCtx, slot); err != nil {
				break loop
			}
		}

		// Prepare batch
		batchCount := 0
		var batchFailed atomic.Bool

		for i := 0; i < p.cfg.BatchSize; i++ {
			if genCtx.Err() != nil {
				break
			}
			offset := i * encodedRecordSize
			recordBuf := slot.buf[offset : offset+encodedRecordSize]

			var synRec SyntheticRecord
			synRec.FillDeterministicPayload(p.cfg.RunID, seq)
			if _, err := synRec.Encode(recordBuf); err != nil {
				runErr = fmt.Errorf("failed to encode record: %w", err)
				break loop
			}

			key := KeyForSeq(seq)
			slot.records[i] = kgo.Record{
				Key:   key,
				Value: recordBuf,
				Topic: p.cfg.Topic,
			}

			seq++
			batchCount++
		}

		if batchCount == 0 {
			break loop
		}

		// Submit batch to Kafka
		slot.wg.Add(batchCount)
		slot.active = true

		for i := 0; i < batchCount; i++ {
			rec := &slot.records[i]
			p.submitted.Add(1)

			// Use background context for delivery so submitted writes can complete during drain.
			// Drain enforces its own bounded timeout (p.cfg.DrainTimeout) over in-flight writes and Flush.
			p.client.Produce(context.Background(), rec, func(r *kgo.Record, err error) {
				defer slot.wg.Done()
				// Only update live counters while the producer run is active.
				// Callbacks arriving after timeout shutdown must not mutate closed run counters.
				if p.closed.Load() {
					return
				}
				if err != nil {
					p.failed.Add(1)
					batchFailed.Store(true)
				} else {
					p.acked.Add(1)
				}
			})
		}

		// Advance to next slot for double buffering
		slotIdx = (slotIdx + 1) % len(slots)

		if batchFailed.Load() && genCtx.Err() == nil {
			runErr = errors.New("producer: batch publication failed")
			break loop
		}
	}

	genElapsed := time.Since(genStart)

	// Bounded Drain & Cleanup Phase:
	// A broker freeze or network partition can leave in-flight writes unresolved.
	// Bounded shutdown must enforce DrainTimeout over the ENTIRE drain phase,
	// including in-flight completions and Flush.
	p.logger.Info("generation finished, starting bounded drain", "drain_timeout", p.cfg.DrainTimeout)
	drainStart := time.Now()

	drainCtx, drainCancel := context.WithTimeout(context.Background(), p.cfg.DrainTimeout)
	defer drainCancel()

	// Wait for all active batch slots to complete or until drainCtx expires
	slotsDone := make(chan struct{})
	go func() {
		for i := range slots {
			if slots[i].active {
				slots[i].wg.Wait()
			}
		}
		close(slotsDone)
	}()

	var drainErr error
	select {
	case <-slotsDone:
		// In-flight batch callbacks completed, now flush any remaining client buffers
		drainErr = p.client.Flush(drainCtx)
	case <-drainCtx.Done():
		drainErr = fmt.Errorf("producer: drain timed out waiting for in-flight completions: %w", drainCtx.Err())
	}

	drainElapsed := time.Since(drainStart)
	totalElapsed := time.Since(genStart)

	// Mark producer run as closed to freeze live counter mutation by any late callbacks
	p.closed.Store(true)

	submittedTotal := p.submitted.Load()
	ackedTotal := p.acked.Load()
	failedObserved := p.failed.Load()

	// Unconfirmed writes are those submitted but not confirmed acknowledged or failed at timeout
	var unconfirmedTotal uint64
	if ackedTotal+failedObserved < submittedTotal {
		unconfirmedTotal = submittedTotal - (ackedTotal + failedObserved)
	}
	failedFinal := failedObserved + unconfirmedTotal

	summary := &Summary{
		RunID:        p.cfg.RunID,
		Submitted:    submittedTotal,
		Acknowledged: ackedTotal,
		Failed:       failedFinal,
		GenElapsed:   genElapsed,
		DrainElapsed: drainElapsed,
		TotalElapsed: totalElapsed,
	}

	if runErr != nil {
		return summary, runErr
	}
	if drainErr != nil {
		return summary, fmt.Errorf("producer: drain failed: %w", drainErr)
	}
	if summary.Failed > 0 {
		return summary, fmt.Errorf("producer: completed with %d failed/unconfirmed records", summary.Failed)
	}

	return summary, nil
}
