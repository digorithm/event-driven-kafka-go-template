package main

import (
	"code-demo/pkg/producer"
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/Shopify/sarama"
)

// Sarama configuration options
var (
	brokers = "127.0.0.1:9092"
	version = "2.1.1"
	group   = "ordering-service"
	topics  = "code-demo-orders"
	oldest  = true
	verbose = false
)

func main() {
	log.Println("Starting a new Sarama consumer")

	version, err := sarama.ParseKafkaVersion(version)
	if err != nil {
		log.Panicf("Error parsing Kafka version: %v", err)
	}

	/**
	 * Construct a new Sarama configuration.
	 * The Kafka cluster version has to be defined before the consumer/producer is initialized.
	 */
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Version = version
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.MaxMessageBytes = 1000012
	config.Producer.RequiredAcks = sarama.WaitForAll

	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin

	if oldest {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	// Setup the producer
	saramaProducer, err := sarama.NewAsyncProducer(strings.Split(brokers, ","), config)
	if err != nil {
		log.Printf("could not initialize sarama producer: %s", err)
		return
	}
	producer := producer.NewProducer(saramaProducer)

	// Setup the consumer
	consumer := Consumer{
		ready:    make(chan bool),
		producer: producer,
	}

	ctx, cancel := context.WithCancel(context.Background())
	client, err := sarama.NewConsumerGroup(strings.Split(brokers, ","), group, config)
	if err != nil {
		log.Panicf("Error creating consumer group client: %v", err)
	}

	// Main loop that will instantiate the consumer group
	// Note that when any consumer returns, the consumer group itself returns
	// and then the loop iterates, creating a new consumer group.
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		// this for loop makes sure the consumer loop is restarted
		for {
			// `Consume` should be called inside an infinite loop, when a
			// server-side rebalance happens, the consumer session will need to be
			// recreated to get the new claims
			if err := client.Consume(ctx, strings.Split(topics, ","), &consumer); err != nil {
				log.Panicf("Error from consumer: %v", err)
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				return
			}
			consumer.ready = make(chan bool)
		}
	}()

	// Reads errors coming from the consumers, in a separate goroutine
	go func() {
		for msg := range client.Errors() {
			fmt.Printf("error coming from channel: %v\n", msg)
		}
	}()

	<-consumer.ready // Wait until the consumer has been set up
	log.Println("Sarama consumer up and running!...")

	// Signal handlers
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-ctx.Done():
		log.Println("terminating: context cancelled")
	case <-sigterm:
		log.Println("terminating: via signal")
	}
	cancel()
	wg.Wait()
	if err = client.Close(); err != nil {
		log.Panicf("Error closing client: %v", err)
	}
}
