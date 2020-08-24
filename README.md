Simple template that I've been using to build event-driven systems using:
1. Go
2. Kafka
3. Sarama's Kafka API (https://github.com/Shopify/sarama)

It's organized as a reusable producer code in a high-level package (`pkg`). This producer can be used embedded in any consumer. 

Then we have the consumers, I created two as an example: `ordering` and `payment`. Creating a new consumer is only about following what has been done in these packages. 
