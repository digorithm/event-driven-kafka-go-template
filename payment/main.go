package main

import (
	"code-demo/pkg/producer"
	"context"
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
	brokers  = "127.0.0.1:9092"
	version  = "2.1.1"
	group    = "payment-service"
	topics   = "code-demo-validated-orders"
	assignor = "roundrobin"
	oldest   = true
	verbose  = false
)

func main() {
	log.Println("Starting a new Sarama consumer")

	//sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)

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

	switch assignor {
	case "sticky":
		config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategySticky
	case "roundrobin":
		config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	case "range":
		config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	default:
		log.Panicf("Unrecognized consumer group partition assignor: %s", assignor)
	}

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

	/**
	 * Setup a new Sarama consumer group
	 */
	consumer := Consumer{
		ready:    make(chan bool),
		producer: producer,
	}

	ctx, cancel := context.WithCancel(context.Background())
	client, err := sarama.NewConsumerGroup(strings.Split(brokers, ","), group, config)
	if err != nil {
		log.Panicf("Error creating consumer group client: %v", err)
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
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

	<-consumer.ready // Await till the consumer has been set up
	log.Println("Sarama consumer up and running!...")

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
