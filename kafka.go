package kafka

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

var (
	once   sync.Once
	writer *kafka.Writer
)

// NewWriter , Initialize a kafka writer and returns it.
func NewWriter(kafkaBrokerUrl string, clientId string, topic string) (*kafka.Writer, error) {
	once.Do(func() {
		w := &kafka.Writer{
			Addr:         tcp(kafkaBrokerUrl),
			Topic:        topic,
			BatchTimeout: 100 * time.Millisecond,
			BatchSize:    5,
			Transport:    kafka.DefaultTransport,
		}

		writer = w
	})
	return writer, nil
}

// Produce ,this method push a key/value pair to the kafka broker.
func Produce(parent context.Context, key, value []byte) error {
	message := kafka.Message{
		Key:   key,
		Value: value,
		Time:  time.Now(),
	}

	return writer.WriteMessages(parent, message)
}

// Consume , this method consumes the published messages to the kafka broker.
func Consume(ctx context.Context, kafkaBrokerUrl string,
	kafkaTopic string,
	kafkaClientId string, brokers []string) (*kafka.Message, error) {

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	reader := NewReaderWithOption(
		WithBrokers(brokers),
		WithTopic(kafkaTopic),
		WithGroupID(kafkaClientId),
		WithMinBytes(10e3), // 10 KB
		WithMaxBytes(10e6), // 10 MB
		WithReadLagInterval(-1),
		WithMaxWait(time.Second*1),
	)
	defer reader.Close()

	dataCh := make(chan kafka.Message, 1)
	errCh := make(chan error, 1)
	for {
		go func() {

			m, err := reader.ReadMessage(ctx)
			if err != nil {
				errCh <- errors.Join(errors.New("error while receiving message"), err)
				return
			}

			dataCh <- m

		}()

		select {
		case m := <-dataCh:
			fmt.Printf("message at topic/partition/offset %v/%v/%v: %s\n",
				m.Topic,
				m.Partition,
				m.Offset,
				string(m.Value))
			return &m, nil
		case <-time.After(time.Second * 20):
			return nil, errors.New("time out")
		case ec := <-errCh:
			return nil, ec
		}
	}
}

type ReaderConfig struct {
	Brokers         []string
	Topic           string
	GroupID         string
	MinBytes        int
	MaxBytes        int
	MaxWait         time.Duration
	ReadLagInterval time.Duration
}

func NewReaderWithOptions(options ReaderConfig) *kafka.Reader {
	// make a new reader that consumes from topic-A
	config := kafka.ReaderConfig{
		Brokers:         options.Brokers,
		GroupID:         options.GroupID,
		Topic:           options.Topic,
		MinBytes:        options.MinBytes,        // 10KB
		MaxBytes:        options.MaxBytes,        // 10MB
		MaxWait:         options.MaxWait,         // Maximum amount of time to wait for new data to come when fetching batches of messages from kafka.
		ReadLagInterval: options.ReadLagInterval, // -1
	}

	reader := kafka.NewReader(config)
	return reader
}

type ReaderOption func(*ReaderConfig)

func WithBrokers(brokers []string) ReaderOption {
	return func(config *ReaderConfig) {
		config.Brokers = brokers
	}
}

func WithTopic(topic string) ReaderOption {
	return func(config *ReaderConfig) {
		config.Topic = topic
	}
}

func WithGroupID(groupID string) ReaderOption {
	return func(config *ReaderConfig) {
		config.GroupID = groupID
	}
}

func WithMinBytes(minBytes int) ReaderOption {
	return func(config *ReaderConfig) {
		config.MinBytes = minBytes
	}
}

func WithMaxBytes(maxBytes int) ReaderOption {
	return func(config *ReaderConfig) {
		config.MaxBytes = maxBytes
	}
}

func WithMaxWait(maxWait time.Duration) ReaderOption {
	return func(config *ReaderConfig) {
		config.MaxWait = maxWait
	}
}

func WithReadLagInterval(readLagInterval time.Duration) ReaderOption {
	return func(config *ReaderConfig) {
		config.ReadLagInterval = readLagInterval
	}
}

func NewReaderWithOption(options ...ReaderOption) *kafka.Reader {
	config := &ReaderConfig{}
	for _, option := range options {
		option(config)
	}

	reader := NewReaderWithOptions(*config)
	return reader
}
