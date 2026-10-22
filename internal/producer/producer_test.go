package producer_test

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/manujkathuria/go-kafka-hbase/internal/producer"
	"github.com/twmb/franz-go/pkg/kgo"
)

type mockKafkaClient struct {
	mu           sync.Mutex
	produced     []*kgo.Record
	produceHook  func(ctx context.Context, r *kgo.Record, promise func(*kgo.Record, error))
	flushErr     error
	closed       bool
	produceCount atomic.Uint64
}

func (m *mockKafkaClient) Produce(ctx context.Context, r *kgo.Record, promise func(*kgo.Record, error)) {
	m.produceCount.Add(1)
	if m.produceHook != nil {
		m.produceHook(ctx, r, promise)
		return
	}
	m.mu.Lock()
	// Copy record content to simulate broker retention and avoid race
	keyCopy := append([]byte(nil), r.Key...)
	valCopy := append([]byte(nil), r.Value...)
	m.produced = append(m.produced, &kgo.Record{
		Key:   keyCopy,
		Value: valCopy,
		Topic: r.Topic,
	})
	m.mu.Unlock()

	promise(r, nil)
}

func (m *mockKafkaClient) Flush(ctx context.Context) error {
	return m.flushErr
}

func (m *mockKafkaClient) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
}

func TestProducerRunNormal(t *testing.T) {
	client := &mockKafkaClient{}

	cfg := producer.Config{
		Brokers:      []string{"localhost:9092"},
		Topic:        "test.topic",
		Duration:     50 * time.Millisecond,
		DrainTimeout: 500 * time.Millisecond,
		BatchSize:    10,
	}

	p, err := producer.New(cfg, client, nil)
	if err != nil {
		t.Fatalf("unexpected New error: %v", err)
	}

	summary, err := p.Run(context.Background())
	if err != nil {
		t.Fatalf("unexpected Run error: %v", err)
	}

	if summary.Submitted == 0 {
		t.Errorf("expected submitted > 0, got %d", summary.Submitted)
	}
	if summary.Acknowledged != summary.Submitted {
		t.Errorf("expected acked == submitted (%d != %d)", summary.Acknowledged, summary.Submitted)
	}
	if summary.Failed != 0 {
		t.Errorf("expected failed == 0, got %d", summary.Failed)
	}
	if summary.GenElapsed <= 0 {
		t.Errorf("expected GenElapsed > 0")
	}

	var out bytes.Buffer
	summary.Print(&out)
	if !bytes.Contains(out.Bytes(), []byte("Producer Execution Summary")) {
		t.Errorf("summary output missing expected header: %s", out.String())
	}
}

func TestProducerRunContextCancellation(t *testing.T) {
	client := &mockKafkaClient{}

	cfg := producer.Config{
		Brokers:      []string{"localhost:9092"},
		Topic:        "test.topic",
		Duration:     10 * time.Second, // Long duration
		DrainTimeout: 500 * time.Millisecond,
		BatchSize:    10,
	}

	p, err := producer.New(cfg, client, nil)
	if err != nil {
		t.Fatalf("unexpected New error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	// Cancel after 30ms
	time.AfterFunc(30*time.Millisecond, cancel)

	summary, err := p.Run(ctx)
	if err != nil {
		t.Fatalf("unexpected error on cancellation: %v", err)
	}

	if summary.Submitted == 0 {
		t.Errorf("expected submitted > 0 before cancellation, got %d", summary.Submitted)
	}
	if summary.Acknowledged != summary.Submitted {
		t.Errorf("expected all submitted to be drained and acked")
	}
}

func TestProducerRunPublicationError(t *testing.T) {
	pubErr := errors.New("simulated broker down")
	client := &mockKafkaClient{
		produceHook: func(ctx context.Context, r *kgo.Record, promise func(*kgo.Record, error)) {
			promise(r, pubErr)
		},
	}

	cfg := producer.Config{
		Brokers:      []string{"localhost:9092"},
		Topic:        "test.topic",
		Duration:     100 * time.Millisecond,
		DrainTimeout: 500 * time.Millisecond,
		BatchSize:    5,
	}

	p, err := producer.New(cfg, client, nil)
	if err != nil {
		t.Fatalf("unexpected New error: %v", err)
	}

	summary, err := p.Run(context.Background())
	if err == nil {
		t.Fatalf("expected error on publication failure, got nil")
	}
	if summary.Failed == 0 {
		t.Errorf("expected failed > 0, got %d", summary.Failed)
	}
	if summary.Acknowledged != 0 {
		t.Errorf("expected acked == 0, got %d", summary.Acknowledged)
	}
}

func TestProducerRunDrainError(t *testing.T) {
	drainErr := errors.New("flush timeout")
	client := &mockKafkaClient{
		flushErr: drainErr,
	}

	cfg := producer.Config{
		Brokers:      []string{"localhost:9092"},
		Topic:        "test.topic",
		Duration:     20 * time.Millisecond,
		DrainTimeout: 100 * time.Millisecond,
		BatchSize:    5,
	}

	p, err := producer.New(cfg, client, nil)
	if err != nil {
		t.Fatalf("unexpected New error: %v", err)
	}

	summary, err := p.Run(context.Background())
	if !errors.Is(err, drainErr) && (err == nil || !bytes.Contains([]byte(err.Error()), []byte("drain failed"))) {
		t.Fatalf("expected drain error, got: %v", err)
	}
	if summary == nil {
		t.Fatalf("expected non-nil summary even on drain error")
	}
}

func TestProducerConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		cfg     producer.Config
		wantErr bool
	}{
		{
			name: "missing brokers",
			cfg: producer.Config{
				Topic:    "topic",
				Duration: time.Second,
			},
			wantErr: true,
		},
		{
			name: "blank broker",
			cfg: producer.Config{
				Brokers:  []string{" "},
				Topic:    "topic",
				Duration: time.Second,
			},
			wantErr: true,
		},
		{
			name: "missing topic",
			cfg: producer.Config{
				Brokers:  []string{"localhost:9092"},
				Duration: time.Second,
			},
			wantErr: true,
		},
		{
			name: "invalid duration",
			cfg: producer.Config{
				Brokers:  []string{"localhost:9092"},
				Topic:    "topic",
				Duration: 0,
			},
			wantErr: true,
		},
		{
			name: "valid with defaults",
			cfg: producer.Config{
				Brokers:  []string{"localhost:9092"},
				Topic:    "topic",
				Duration: 5 * time.Second,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
