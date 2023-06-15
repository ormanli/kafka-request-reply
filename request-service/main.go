package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

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

	client, err := sarama.NewClient([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatalln("Failed to start Sarama client:", err)
	}

	partitions, err := client.Partitions(kafka.TopicHelloReply)
	if err != nil {
		log.Fatalln("Failed to get partitions:", err)
	}

	consumerGroup, err := sarama.NewConsumerGroup([]string{"localhost:9092"}, "request-service", config)
	if err != nil {
		log.Fatalln("Failed to start Sarama consumer:", err)
	}

	defer consumerGroup.Close()

	helloReplyConsumer := &helloReplyConsumer{
		ready:   make(chan bool),
		returns: make(map[string]chan<- string),
	}

	go func() {
		for {
			if err := consumerGroup.Consume(ctx, []string{kafka.TopicHelloReply}, helloReplyConsumer); err != nil {
				log.Panicf("Error from consumer: %v", err)
			}
			if ctx.Err() != nil {
				return
			}
			helloReplyConsumer.ready = make(chan bool)
		}
	}()

	<-helloReplyConsumer.ready

	http.HandleFunc("/hello", helloHandler(producer, helloReplyConsumer, partitions[0]))

	log.Printf("Request service started")
	http.ListenAndServe(":8080", nil)
}

func helloHandler(producer sarama.SyncProducer, consumer *helloReplyConsumer, partition int32) func(w http.ResponseWriter, req *http.Request) {
	return func(w http.ResponseWriter, req *http.Request) {
		defer req.Body.Close()
		b, _ := io.ReadAll(req.Body)

		m := &kafka.RequestMessage{}
		json.Unmarshal(b, m)

		msg := &sarama.ProducerMessage{
			Topic: kafka.TopicHelloRequest,
			Value: sarama.ByteEncoder(b),
			Headers: []sarama.RecordHeader{
				{Key: []byte(kafka.HeaderReplyPartition), Value: []byte(fmt.Sprintf("%d", partition))},
				{Key: []byte(kafka.HeaderReplyTopic), Value: []byte(kafka.TopicHelloReply)},
			},
		}

		returnChannel := consumer.Add(m.Name)

		producer.SendMessage(msg)

		select {
		case res := <-returnChannel:
			w.Write([]byte(fmt.Sprintf("Reply service says: %q", res)))
		case <-time.After(1 * time.Second):
			w.Write([]byte(fmt.Sprintf("Got timeout for %q", m.Name)))
		}
	}
}

type helloReplyConsumer struct {
	ready   chan bool
	returns map[string]chan<- string
}

func (c *helloReplyConsumer) Add(name string) <-chan string {
	returnChannel := make(chan string, 1)
	c.returns[name] = returnChannel

	return returnChannel
}

func (c *helloReplyConsumer) Setup(sarama.ConsumerGroupSession) error {
	close(c.ready)
	return nil
}

func (c *helloReplyConsumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *helloReplyConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message := <-claim.Messages():
			session.MarkMessage(message, "")

			m := &kafka.ReplyMessage{}
			err := json.Unmarshal(message.Value, m)
			if err != nil {
				return err
			}

			name := ""
			for _, header := range message.Headers {
				if string(header.Key) == kafka.HeaderReplyTo {
					name = string(header.Value)
				}
			}

			c.returns[name] <- m.Says
		case <-session.Context().Done():
			return nil
		}
	}
}
