package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
)

func main() {
	ctx, cncl := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cncl()

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	kafkaHostPort := os.Getenv("KAFKA_HOST_PORT")
	if kafkaHostPort == "" {
		kafkaHostPort = "localhost:9092"
	}

	consumerGroup, err := sarama.NewConsumerGroup([]string{kafkaHostPort}, "request-service", config)
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
			if err := consumerGroup.Consume(ctx, []string{TopicHelloReply}, helloReplyConsumer); err != nil {
				log.Panicf("Error from consumer: %v", err)
			}
			if ctx.Err() != nil {
				return
			}
			helloReplyConsumer.ready = make(chan bool)
		}
	}()

	<-helloReplyConsumer.ready

	producer, err := sarama.NewSyncProducer([]string{kafkaHostPort}, config)
	if err != nil {
		log.Fatalln("Failed to start Sarama producer:", err)
	}

	defer producer.Close()

	http.HandleFunc("/hello", helloHandler(producer, helloReplyConsumer))

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("Request service started on %s", port)
	http.ListenAndServe(fmt.Sprintf(":%s", port), nil)
}

func helloHandler(producer sarama.SyncProducer, consumer *helloReplyConsumer) func(w http.ResponseWriter, req *http.Request) {
	return func(w http.ResponseWriter, req *http.Request) {
		defer req.Body.Close()
		b, _ := io.ReadAll(req.Body)

		m := &RequestMessage{}
		json.Unmarshal(b, m)

		replyPartition := consumer.randomPartition()
		msg := &sarama.ProducerMessage{
			Topic: TopicHelloRequest,
			Value: sarama.ByteEncoder(b),
			Headers: []sarama.RecordHeader{
				{Key: []byte(HeaderReplyPartition), Value: []byte(fmt.Sprintf("%d", replyPartition))},
				{Key: []byte(HeaderReplyTopic), Value: []byte(TopicHelloReply)},
			},
		}

		returnChannel := consumer.register(m.Name)

		log.Printf("Send request for %q to get reply [%s,%d]", m.Name, TopicHelloReply, replyPartition)

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
	ready                chan bool
	returns              map[string]chan<- string
	mu                   sync.Mutex
	partitions           atomic.Pointer[[]int32]
	randomPartitionIndex int64
}

func (c *helloReplyConsumer) register(name string) <-chan string {
	c.mu.Lock()
	defer c.mu.Unlock()
	returnChannel := make(chan string, 1)
	c.returns[name] = returnChannel

	return returnChannel
}

func (c *helloReplyConsumer) randomPartition() int32 {
	partitions := *c.partitions.Load()
	defer atomic.AddInt64(&c.randomPartitionIndex, 1)

	return partitions[int(c.randomPartitionIndex)%len(partitions)]
}

func (c *helloReplyConsumer) Setup(cgs sarama.ConsumerGroupSession) error {
	partitions := cgs.Claims()[TopicHelloReply]
	c.partitions.Store(&partitions)
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

			m := &ReplyMessage{}
			err := json.Unmarshal(message.Value, m)
			if err != nil {
				return err
			}

			name := ""
			for _, header := range message.Headers {
				if string(header.Key) == HeaderReplyTo {
					name = string(header.Value)
				}
			}

			c.mu.Lock()
			replyChannel := c.returns[name]
			delete(c.returns, name)
			c.mu.Unlock()

			replyChannel <- m.Says
			close(replyChannel)

			log.Printf("Received reply for %q from [%s,%d]", name, claim.Topic(), claim.Partition())
		case <-session.Context().Done():
			return nil
		}
	}
}

const TopicHelloRequest = "hello_request"
const TopicHelloReply = "hello_reply"
const HeaderReplyTopic = "reply_topic"
const HeaderReplyPartition = "reply_partition"
const HeaderReplyTo = "reply_to"

type RequestMessage struct {
	Name string `json:"name"`
}

type ReplyMessage struct {
	Says string `json:"says"`
}
