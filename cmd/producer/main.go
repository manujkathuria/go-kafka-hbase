package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/manujkathuria/go-kafka-hbase/internal/producer"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	var (
		brokersFlag = flag.String("brokers", "localhost:9092", "Comma-separated list of Kafka broker addresses")
		topicFlag   = flag.String("topic", "", "Target Kafka topic (required, must exist)")
		durFlag     = flag.Duration("duration", 10*time.Second, "Workload generation duration (e.g. 10s, 1m)")
		batchFlag   = flag.Int("batch-size", producer.DefaultBatchSize, "Maximum number of records per reusable batch")
		drainFlag   = flag.Duration("drain-timeout", producer.DefaultDrainTimeout, "Maximum duration to wait for outstanding writes to drain")
	)

	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	if strings.TrimSpace(*topicFlag) == "" {
		fmt.Fprintf(os.Stderr, "Error: -topic flag is required\n\nUsage:\n")
		flag.PrintDefaults()
		os.Exit(2)
	}

	brokerAddrs := strings.Split(*brokersFlag, ",")
	for i := range brokerAddrs {
		brokerAddrs[i] = strings.TrimSpace(brokerAddrs[i])
	}

	cfg := producer.Config{
		Brokers:      brokerAddrs,
		Topic:        *topicFlag,
		Duration:     *durFlag,
		DrainTimeout: *drainFlag,
		BatchSize:    *batchFlag,
	}

	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "Configuration error: %v\n", err)
		os.Exit(2)
	}

	// Initialize franz-go client with reliable settings:
	// - Require all ISR acknowledgements (AllISRAcks)
	// - Enable idempotence (ProducerBatchCompression, ProduceRequestTimeout, etc.)
	// - Client-side backpressure using MaxBufferedRecords
	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.DefaultProduceTopic(cfg.Topic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		// Franz-go default partitioner is uniform murmur2 hash, consistent with Kafka
		kgo.ProducerBatchCompression(kgo.Lz4Compression()),
		kgo.MaxBufferedRecords(cfg.BatchSize*2),
	)
	if err != nil {
		logger.Error("failed to initialize Kafka client", "error", err)
		os.Exit(1)
	}
	defer client.Close()

	// Verify broker connectivity before generating work
	ctxPing, cancelPing := context.WithTimeout(context.Background(), 5*time.Second)
	if err := client.Ping(ctxPing); err != nil {
		cancelPing()
		logger.Error("failed to connect to Kafka brokers", "brokers", cfg.Brokers, "error", err)
		os.Exit(1)
	}
	cancelPing()

	// Root context with graceful interrupt handling (SIGINT / SIGTERM)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	prod, err := producer.New(cfg, client, logger)
	if err != nil {
		logger.Error("failed to create producer", "error", err)
		os.Exit(1)
	}

	summary, runErr := prod.Run(ctx)
	if summary != nil {
		summary.Print(os.Stdout)
	}

	if runErr != nil {
		logger.Error("producer finished with error", "error", runErr)
		os.Exit(1)
	}
}
