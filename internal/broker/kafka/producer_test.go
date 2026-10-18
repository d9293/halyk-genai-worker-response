package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/IBM/sarama"
	"github.com/IBM/sarama/mocks"
)

func newMocked(t *testing.T) (*Producer, *mocks.SyncProducer) {
	t.Helper()
	m := mocks.NewSyncProducer(t, nil)
	return &Producer{producer: m}, m
}

func TestPublishSendsKeyAndValueUnchanged(t *testing.T) {
	p, m := newMocked(t)
	value := []byte(`{"requestGuid":"g1",  "result":{"b":1,"a":2.50}}`)
	m.ExpectSendMessageWithMessageCheckerFunctionAndSucceed(func(msg *sarama.ProducerMessage) error {
		key, _ := msg.Key.Encode()
		got, _ := msg.Value.Encode()
		if msg.Topic != "integrator.task-results.v1" || string(key) != "r1" || string(got) != string(value) {
			return errors.New("unexpected message")
		}
		return nil
	})

	if err := p.Publish(context.Background(), "integrator.task-results.v1", "r1", value); err != nil {
		t.Fatal(err)
	}
}

func TestPublishErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want error
	}{
		{"too large", sarama.ErrMessageSizeTooLarge, ErrTooLarge},
		{"broker down", sarama.ErrOutOfBrokers, ErrUnavailable},
		{"not enough replicas", sarama.ErrNotEnoughReplicas, ErrUnavailable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, m := newMocked(t)
			m.ExpectSendMessageAndFail(tt.err)
			if err := p.Publish(context.Background(), "t", "k", []byte("{}")); !errors.Is(err, tt.want) {
				t.Errorf("err = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestPublishNotStartedOnShutdown(t *testing.T) {
	p, _ := newMocked(t) // no expectation: a send would fail the test
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := p.Publish(ctx, "t", "k", []byte("{}")); !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v", err)
	}
}
