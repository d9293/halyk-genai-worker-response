package kafka

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/IBM/sarama"
)

// TestContract publishes to a real Kafka and reads the message back. Skipped
// unless CONTRACT_KAFKA_BROKERS is set:
//
//	CONTRACT_KAFKA_BROKERS=localhost:29092 go test -run Contract ./internal/broker/kafka/
//
// It writes to the topic worker-response.contract-test (auto-created).
func TestContract(t *testing.T) {
	brokers := os.Getenv("CONTRACT_KAFKA_BROKERS")
	if brokers == "" {
		t.Skip("CONTRACT_KAFKA_BROKERS not set")
	}
	const topic = "worker-response.contract-test"
	cfg := Config{Brokers: strings.Split(brokers, ","), Version: "3.9.0", ClientID: "contract-test", Timeout: 10 * time.Second}
	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	key := "r-" + time.Now().Format("150405.000000")
	value := []byte(`{"schemaVersion":1,"requestGuid":"g1",  "result":{"b":1,"a":2.50}}`)
	if err := p.Publish(context.Background(), topic, key, value); err != nil {
		t.Fatalf("publish: %v", err)
	}

	consumer, err := sarama.NewConsumer(cfg.Brokers, sarama.NewConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	partitions, err := consumer.Partitions(topic)
	if err != nil {
		t.Fatal(err)
	}
	found := make(chan *sarama.ConsumerMessage, 1)
	for _, partition := range partitions {
		pc, err := consumer.ConsumePartition(topic, partition, sarama.OffsetOldest)
		if err != nil {
			t.Fatal(err)
		}
		defer pc.Close()
		go func() {
			for msg := range pc.Messages() {
				if string(msg.Key) == key {
					found <- msg
					return
				}
			}
		}()
	}
	select {
	case msg := <-found:
		if string(msg.Value) != string(value) {
			t.Errorf("value changed:\n got %s\nwant %s", msg.Value, value)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("published message not found in the topic")
	}
}
