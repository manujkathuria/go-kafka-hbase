package producer

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// closeClient completes pending records only when its owner closes transport.
// With complete=false it deliberately never invokes callbacks: Run must not
// create waiter goroutines that depend on those callbacks for their lifetime.
type closeClient struct {
	callbacks []func()
	complete  bool
	closed    bool
	onClose   func()
}

func (c *closeClient) Produce(_ context.Context, r *kgo.Record, cb func(*kgo.Record, error)) {
	c.callbacks = append(c.callbacks, func() { cb(r, errors.New("transport closed")) })
}
func (c *closeClient) Flush(context.Context) error { return nil }
func (c *closeClient) Close() {
	c.closed = true
	if c.onClose != nil {
		c.onClose()
	}
	if c.complete {
		var wg sync.WaitGroup
		for _, cb := range c.callbacks {
			wg.Add(1)
			go func() { defer wg.Done(); cb() }()
		}
		wg.Wait()
	}
}

func TestShutdownOwnsClientAndFreezesInternalCounters(t *testing.T) {
	for _, complete := range []bool{false, true} {
		t.Run(map[bool]string{false: "never-completes", true: "callbacks-during-close"}[complete], func(t *testing.T) {
			client := &closeClient{complete: complete}
			p, err := New(Config{Brokers: []string{"unused"}, Topic: "test", Duration: 10 * time.Millisecond, DrainTimeout: 20 * time.Millisecond, BatchSize: 2}, client, nil)
			if err != nil {
				t.Fatal(err)
			}
			client.onClose = func() {
				p.mu.Lock()
				defer p.mu.Unlock()
				if !p.closed {
					t.Error("accounting must be finalized before transport cleanup")
				}
			}
			type result struct {
				summary *Summary
				err     error
			}
			done := make(chan result, 1)
			go func() { s, e := p.Run(context.Background()); done <- result{s, e} }()
			var got result
			select {
			case got = <-done:
			case <-time.After(time.Second):
				t.Fatal("shutdown blocked")
			}
			if !errors.Is(got.err, context.DeadlineExceeded) {
				t.Fatalf("expected drain deadline, got %v", got.err)
			}
			if !client.closed {
				t.Fatal("client was not closed before return")
			}
			if got.summary.Submitted != 4 || got.summary.Acknowledged != 0 || got.summary.Failed != 4 {
				t.Fatalf("unexpected summary: %+v", got.summary)
			}
			p.mu.Lock()
			defer p.mu.Unlock()
			if p.acked != 0 || p.failed != 0 {
				t.Fatalf("late callbacks changed internal counters: acked=%d failed=%d", p.acked, p.failed)
			}
		})
	}
}
