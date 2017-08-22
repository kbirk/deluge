package kafka

import (
	"fmt"
	"io"

	"github.com/optiopay/kafka"
)

// ClientOptionFunc is a function that configures an Kafka client.
type ClientOptionFunc func(*Client) error

// SetURLs sets the endpoint URL.
func SetURLs(urls []string) ClientOptionFunc {
	return func(c *Client) error {
		if len(urls) > 0 {
			c.endpoints = urls
		}
		return nil
	}
}

// SetUser sets the user.
func SetUser(user string) ClientOptionFunc {
	return func(c *Client) error {
		if user != "" {
			c.user = user
		}
		return nil
	}
}

// SetTopic sets the topic.
func SetTopic(topic string) ClientOptionFunc {
	return func(c *Client) error {
		if topic != "" {
			c.topic = topic
		}
		return nil
	}
}

// Client represents an Kafka client.
type Client struct {
	endpoints     []string
	user          string
	topic         string
	broker        *kafka.Broker
	numPartitions int
	consumerIndex int
	consumers     []kafka.Consumer
}

// NewClient instantiates and returns a new Kafka client.
func NewClient(options ...ClientOptionFunc) (*Client, error) {
	// instantiate client
	client := &Client{
		endpoints: []string{"localhost:9092"},
		user:      "default-user",
		topic:     "",
	}
	// run the options through it
	for _, option := range options {
		if err := option(client); err != nil {
			return nil, err
		}
	}
	if client.topic == "" {
		return nil, fmt.Errorf("missing topic argument")
	}
	// create broker config
	conf := kafka.NewBrokerConf(client.user)
	// connect to kafka cluster
	broker, err := kafka.Dial(client.endpoints, conf)
	if err != nil {
		return nil, err
	}
	client.broker = broker
	// get number of partitions
	numPartitions, err := broker.PartitionCount(client.topic)
	if err != nil {
		return nil, err
	}
	client.numPartitions = int(numPartitions)
	// create consumer for each partition
	for i := 0; i < client.numPartitions; i++ {
		// create consumer config
		conf := kafka.NewConsumerConf(client.topic, int32(i))
		// create consumer
		consumer, err := broker.Consumer(conf)
		if err != nil {
			return nil, err
		}
		client.consumers = append(client.consumers, consumer)
	}
	return client, nil
}

// Close closes the underlying connection.
func (c *Client) Close() error {
	c.broker.Close()
	return nil
}

// Consume consumes and returns the next portion of the topic.
func (c *Client) Consume() ([]byte, error) {
	msg, err := c.consumers[c.consumerIndex].Consume()
	if err != nil {
		if err == kafka.ErrNoData {
			return nil, io.EOF
		}
		return nil, err
	}
	c.consumerIndex += c.consumerIndex % c.numPartitions
	return msg.Value, nil
}
