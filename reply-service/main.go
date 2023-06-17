package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"

	"github.com/Shopify/sarama"
)

func main() {
	ctx, cncl := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cncl()

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.Partitioner = sarama.NewManualPartitioner

	kafkaHostPort := os.Getenv("KAFKA_HOST_PORT")
	if kafkaHostPort == "" {
		kafkaHostPort = "localhost:9092"
	}

	producer, err := sarama.NewSyncProducer([]string{kafkaHostPort}, config)
	if err != nil {
		log.Fatalln("Failed to start Sarama producer:", err)
	}

	defer producer.Close()

	consumerGroup, err := sarama.NewConsumerGroup([]string{kafkaHostPort}, "reply-service", config)
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
			if err := consumerGroup.Consume(ctx, []string{TopicHelloRequest}, helloRequestConsumer); err != nil {
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

			requestMessage := &RequestMessage{}
			err := json.Unmarshal(message.Value, requestMessage)
			if err != nil {
				return err
			}

			topic := ""
			partition := 0

			for _, header := range message.Headers {
				if string(header.Key) == HeaderReplyTopic {
					topic = string(header.Value)
				} else if string(header.Key) == HeaderReplyPartition {
					partition, _ = strconv.Atoi(string(header.Value))
				}
			}

			replyMessage := &ReplyMessage{Says: fmt.Sprintf("Hello, %s", requestMessage.Name)}
			b, err := json.Marshal(replyMessage)
			if err != nil {
				return err
			}

			msg := &sarama.ProducerMessage{
				Topic: topic,
				Value: sarama.ByteEncoder(b),
				Headers: []sarama.RecordHeader{
					{Key: []byte(HeaderReplyTo), Value: []byte(requestMessage.Name)},
				},
				Partition: int32(partition),
			}

			_, _, err = c.producer.SendMessage(msg)
			if err != nil {
				log.Printf("error: %s", err)
			}

			log.Printf("Replied to %q from [%s,%d] to [%s,%d]", requestMessage.Name, claim.Topic(), claim.Partition(), topic, partition)
		case <-session.Context().Done():
			return nil
		}
	}
}

const TopicHelloRequest = "hello_request"
const HeaderReplyTopic = "reply_topic"
const HeaderReplyPartition = "reply_partition"
const HeaderReplyTo = "reply_to"

type RequestMessage struct {
	Name string `json:"name"`
}

type ReplyMessage struct {
	Says string `json:"says"`
}
