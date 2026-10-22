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

// Run executes the generation loop until ctx is canceled (or duration expires),
// then performs a bounded drain. Returns Summary and any terminal error.
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

	// We use reusable bounded batch buffers.
	// To preserve memory safety across asynchronous Franz-go completion:
	// Franz-go retains key and value byte slices until the produce promise callback is invoked.
	// Therefore, each in-flight batch must keep its memory valid until all records in that batch complete.
	// A synchronous batch loop (wait for batch acknowledgements before reusing the batch buffer)
	// gives bounded memory, backpressure from Kafka, and simple, completely safe ownership.
	encodedRecordSize := RecordHeaderSize + FixedPayloadSize
	batchBuffer := make([]byte, p.cfg.BatchSize*encodedRecordSize)
	records := make([]kgo.Record, p.cfg.BatchSize)

	var seq uint64

loop:
	for {
		select {
		case <-genCtx.Done():
			break loop
		default:
		}

		// Prepare a batch
		batchCount := 0
		var wg sync.WaitGroup
		var batchFailed atomic.Bool

		for i := 0; i < p.cfg.BatchSize; i++ {
			if genCtx.Err() != nil {
				break
			}
			offset := i * encodedRecordSize
			recordBuf := batchBuffer[offset : offset+encodedRecordSize]

			var synRec SyntheticRecord
			synRec.FillDeterministicPayload(p.cfg.RunID, seq)
			if _, err := synRec.Encode(recordBuf); err != nil {
				runErr = fmt.Errorf("failed to encode record: %w", err)
				break loop
			}

			key := KeyForSeq(seq)
			records[i] = kgo.Record{
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

		// Publish the batch
		wg.Add(batchCount)
		for i := 0; i < batchCount; i++ {
			rec := &records[i]
			p.submitted.Add(1)

			p.client.Produce(genCtx, rec, func(r *kgo.Record, err error) {
				defer wg.Done()
				if err != nil {
					p.failed.Add(1)
					batchFailed.Store(true)
					// Avoid hot-path logging every single error; log if needed
				} else {
					p.acked.Add(1)
				}
			})
		}

		// Synchronously wait for batch completion before reusing batchBuffer.
		// If genCtx is canceled while waiting, the wait still needs to complete or bound cleanup.
		wg.Wait()

		if batchFailed.Load() && genCtx.Err() == nil {
			// Publication failure during generation
			runErr = errors.New("producer: batch publication failed")
			break loop
		}
	}

	genElapsed := time.Since(genStart)

	// Bounded Drain using a fresh context
	p.logger.Info("generation finished, starting drain", "drain_timeout", p.cfg.DrainTimeout)
	drainStart := time.Now()

	drainCtx, drainCancel := context.WithTimeout(context.Background(), p.cfg.DrainTimeout)
	defer drainCancel()

	drainErr := p.client.Flush(drainCtx)
	drainElapsed := time.Since(drainStart)

	totalElapsed := time.Since(genStart)

	summary := &Summary{
		RunID:        p.cfg.RunID,
		Submitted:    p.submitted.Load(),
		Acknowledged: p.acked.Load(),
		Failed:       p.failed.Load(),
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
