package producer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
)

type Producer struct {
	saramaProducer sarama.AsyncProducer
}

type Msg struct {
	Key     []byte            // optional, used as Kafka msg key and for internal routing
	Value   []byte            // The kafka message payload
	Headers map[string][]byte // extra headers
}

type kafkaMsgMetadata struct {
	result    chan<- error
	timestamp time.Time
}

// NewProducer returns and instance of Producer, with it's various goroutines launched
func NewProducer(saramaProducer sarama.AsyncProducer) *Producer {
	producer := Producer{
		saramaProducer: saramaProducer,
	}

	go producer.handleErrors()
	go producer.handleSuccesses()

	return &producer
}

// Write a msg to a kafka topic. This sends the message directly to Sarama's Input() channel and then waits for either
// an ack, or for the context to expire.
func (producer *Producer) Write(ctx context.Context, msg Msg, topic string) error {
	kmsg, result, err := producer.buildMessage(msg, topic)
	if err != nil {
		return fmt.Errorf("could not build message: %s", err)
	}

	select {
	case producer.saramaProducer.Input() <- &kmsg:
	case <-ctx.Done():
		return errors.New("message write failed: no space in processing channel")
	}

	select {
	case err, ok := <-result:
		if !ok {
			return errors.New("result channel was closed, write result unknown")
		}
		return err
	case <-ctx.Done():
		return errors.New("message write failed: context expired, write result unknown, not waiting for it")
	}
}

func (producer *Producer) handleSuccesses() {
	for msg := range producer.saramaProducer.Successes() {
		meta, ok := msg.Metadata.(kafkaMsgMetadata)
		if !ok {
			continue
		}
		meta.result <- nil
	}
}

func (producer *Producer) handleErrors() {
	for err := range producer.saramaProducer.Errors() {
		meta, ok := err.Msg.Metadata.(kafkaMsgMetadata)
		if !ok {
			continue
		}
		meta.result <- err
	}
}

func (producer *Producer) buildMessage(msg Msg, topic string) (sarama.ProducerMessage, <-chan error, error) {
	result := make(chan error, 1)

	m := sarama.ProducerMessage{
		Value: sarama.ByteEncoder(msg.Value),
		Topic: topic,
		Metadata: kafkaMsgMetadata{
			result:    result,
			timestamp: time.Now().UTC(),
		},
	}

	return m, result, nil
}
