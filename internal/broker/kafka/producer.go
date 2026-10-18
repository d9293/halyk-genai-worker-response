// Package kafka publishes task results to Kafka with sarama. It is the only
// package that knows sarama, so switching to the company's Kafka library
// touches only this file.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/IBM/sarama"
)

var (
	// ErrTooLarge: the broker refuses the message as too big. Retrying will not help.
	ErrTooLarge = errors.New("message too large for Kafka")
	// ErrUnavailable: Kafka did not confirm the message; it may work later.
	ErrUnavailable = errors.New("kafka unavailable")
)

type Config struct {
	// Brokers, e.g. localhost:29092.
	Brokers []string
	// Version of the brokers, e.g. 3.9.0; sarama needs it for idempotent writes.
	Version string
	// ClientID names this worker in the broker's logs and metrics.
	ClientID string
	// Timeout bounds one publish: waiting for the replicas and every network call.
	Timeout time.Duration
}

type Producer struct {
	producer sarama.SyncProducer
}

// New connects to Kafka. It fails if no broker can be reached, so a
// misconfigured worker does not start.
func New(cfg Config) (*Producer, error) {
	version, err := sarama.ParseKafkaVersion(cfg.Version)
	if err != nil {
		return nil, fmt.Errorf("kafka version %q: %w", cfg.Version, err)
	}

	sc := sarama.NewConfig()
	sc.Version = version
	sc.ClientID = cfg.ClientID
	// A message counts as published only when every in-sync replica has it.
	sc.Producer.RequiredAcks = sarama.WaitForAll
	sc.Producer.Timeout = cfg.Timeout
	// Idempotent writes: sarama's own retries never store a message twice.
	// Sarama requires one request in flight per broker for this.
	sc.Producer.Idempotent = true
	sc.Net.MaxOpenRequests = 1
	sc.Producer.Retry.Max = 3
	sc.Producer.Retry.Backoff = 250 * time.Millisecond
	sc.Producer.Return.Successes = true // required by the sync producer
	// The default partitioner hashes the key (the Camunda requestId), so all
	// messages of one Camunda request land in one partition, in order.
	sc.Net.DialTimeout = cfg.Timeout
	sc.Net.ReadTimeout = cfg.Timeout
	sc.Net.WriteTimeout = cfg.Timeout

	p, err := sarama.NewSyncProducer(cfg.Brokers, sc)
	if err != nil {
		return nil, fmt.Errorf("connect to kafka %v: %w", cfg.Brokers, err)
	}
	return &Producer{producer: p}, nil
}

// Publish writes one message and waits until Kafka confirms it. The value is
// sent exactly as given.
//
// Sarama's sync producer cannot be cancelled; a publish is bounded by
// Config.Timeout and the retries instead. ctx only prevents starting one
// during shutdown.
func (p *Producer) Publish(ctx context.Context, topic, key string, value []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_, _, err := p.producer.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(value),
	})
	switch {
	case err == nil:
		return nil
	case errors.Is(err, sarama.ErrMessageSizeTooLarge):
		return fmt.Errorf("%w: %w", ErrTooLarge, err)
	}
	return fmt.Errorf("%w: %w", ErrUnavailable, err)
}

func (p *Producer) Close() error {
	return p.producer.Close()
}
