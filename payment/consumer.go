package main

import (
	"code-demo/pkg/producer"
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/Shopify/sarama"
	"github.com/cenkalti/backoff"
)

// Consumer represents a Sarama consumer group consumer
type Consumer struct {
	ready    chan bool
	producer *producer.Producer
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	var order Order
	for message := range claim.Messages() {
		err := json.Unmarshal(message.Value, &order)
		if err != nil {
			log.Printf("failed to unmarshall order: %v", err)
			// IMPORTANT: It's upstream convention to mark the offset as currentOffset+1
			session.MarkOffset(claim.Topic(),
				claim.Partition(), message.Offset+1, "")
			continue
		}

		log.Println("### new order payment ###")

		// Create the exponential backoff channel
		expBackoff := backoff.NewExponentialBackOff()
		retry := backoff.NewTicker(expBackoff)

		// Retry loop, if successful at first it will run just once
		for range retry.C {
			// In case the Sarama's context has been canceled
			if session.Context().Err() != nil {
				return nil
			}

			// Our action that could cause a transient error
			if err := ProcessOrderPayment(order); err != nil {
				log.Printf("couldn't process payment: %s. Retrying in %s", err, expBackoff.NextBackOff())

				// If we failed, go to the next retry loop iteration
				continue

			}

			// If we succeeded, stop
			retry.Stop()
			break
		}

		order.Status = "Confirmed"
		orderJson, err := json.Marshal(order)
		if err != nil {
			log.Printf("failed to marshall json")
		}

		// Write to next topic (`code-demo-confirmed-orders`)
		// Retries will happen in here
		consumer.WriteToOutgoingTopic(orderJson, session)

		log.Println("### order confirmed ###")

		// temp
		session.MarkOffset(claim.Topic(),
			claim.Partition(), message.Offset+1, "")

	}

	return nil
}

func (consumer *Consumer) WriteToOutgoingTopic(msg []byte,
	session sarama.ConsumerGroupSession) {

	Msg := producer.Msg{Value: msg}
	d := time.Now().Add(1500 * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), d)
	defer cancel()

	// Create the exponential backoff channel
	expBackoff := backoff.NewExponentialBackOff()
	retry := backoff.NewTicker(expBackoff)

	// Retry loop, if successful at first it will run just once
	for range retry.C {
		// In case the Sarama's context has been canceled
		if session.Context().Err() != nil {
			return
		}

		// Our action that could cause a transient error
		if err := consumer.producer.Write(ctx,
			Msg, "code-demo-confirmed-orders"); err != nil {
			log.Printf("failed to write to confirmed orders topic")
		}

		// If we succeeded, stop
		retry.Stop()
		break
	}

}
