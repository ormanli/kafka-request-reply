package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os/signal"
	"strconv"
	"sync"
	"syscall"

	"github.com/Shopify/sarama"
	"github.com/ormanli/kafka-request-reply"
)

func main() {
	ctx, cncl := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cncl()

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatalln("Failed to start Sarama producer:", err)
	}

	defer producer.Close()

	consumerGroup, err := sarama.NewConsumerGroup([]string{"localhost:9092"}, "reply-service", config)
	if err != nil {
		log.Fatalln("Failed to start Sarama consumer:", err)
	}

	defer consumerGroup.Close()

	helloRequestConsumer := &helloRequestConsumer{
		ready:    make(chan bool),
		producer: producer,
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if err := consumerGroup.Consume(ctx, []string{kafka.TopicHelloRequest}, helloRequestConsumer); err != nil {
				log.Panicf("Error from consumer: %v", err)
			}
			if ctx.Err() != nil {
				return
			}
			helloRequestConsumer.ready = make(chan bool)
		}
	}()

	<-helloRequestConsumer.ready
	log.Printf("Reply service started")
	wg.Wait()
}

type helloRequestConsumer struct {
	ready    chan bool
	producer sarama.SyncProducer
}

func (c *helloRequestConsumer) Setup(sarama.ConsumerGroupSession) error {
	close(c.ready)
	return nil
}

func (c *helloRequestConsumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *helloRequestConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message := <-claim.Messages():
			session.MarkMessage(message, "")

			requestMessage := &kafka.RequestMessage{}
			err := json.Unmarshal(message.Value, requestMessage)
			if err != nil {
				return err
			}

			topic := ""
			partition := 0

			for _, header := range message.Headers {
				if string(header.Key) == kafka.HeaderReplyTopic {
					topic = string(header.Value)
				} else if string(header.Key) == kafka.HeaderReplyPartition {
					partition, _ = strconv.Atoi(string(header.Value))
				}
			}

			replyMessage := &kafka.ReplyMessage{Says: fmt.Sprintf("Hello, %s", requestMessage.Name)}
			b, err := json.Marshal(replyMessage)
			if err != nil {
				return err
			}

			msg := &sarama.ProducerMessage{
				Topic: topic,
				Value: sarama.ByteEncoder(b),
				Headers: []sarama.RecordHeader{
					{Key: []byte(kafka.HeaderReplyTo), Value: []byte(requestMessage.Name)},
				},
				Partition: int32(partition),
			}

			c.producer.SendMessage(msg)
		case <-session.Context().Done():
			return nil
		}
	}
}
