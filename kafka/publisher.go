package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/IBM/sarama"
	"github.com/andryhardiyanto/go-event"
	goLogger "github.com/andryhardiyanto/go-logger"
)

type publisher struct {
	broker   string
	timeout  time.Duration
	logger   goLogger.Logger
	producer sarama.SyncProducer
}

func NewPublisher(opts ...event.EventOption) (event.Publisher, error) {
	if len(opts) == 0 {
		return nil, fmt.Errorf("no options provided")
	}

	cfg := &event.EventConfig{}

	for _, opt := range opts {
		opt(cfg)
	}

	if cfg.Kafka.Publisher.Broker == "" {
		return nil, fmt.Errorf("kafka broker is required")
	}
	if cfg.Logger == nil {
		return nil, fmt.Errorf("Logger is required")
	}

	broker := cfg.Kafka.Publisher.Broker
	logger := cfg.Logger

	timeout := cfg.Kafka.Publisher.Timeout
	if timeout == 0 {
		timeout = time.Second * 5
	}

	requiredAcks := cfg.Kafka.Publisher.RequiredAcks
	if requiredAcks == 0 {
		requiredAcks = sarama.WaitForAll
	}

	retryMax := cfg.Kafka.Publisher.RetryMax
	if retryMax == 0 {
		retryMax = 2
	}

	successes := cfg.Kafka.Publisher.Successes
	if successes {
		successes = true
	}

	maxOpenRequests := cfg.Kafka.Publisher.MaxOpenRequests
	if maxOpenRequests == 0 {
		maxOpenRequests = 1
	}

	kafkaVersion := sarama.V3_6_1_0
	if cfg.Kafka.Version != nil {
		kafkaVersion = *cfg.Kafka.Version
	}

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = requiredAcks
	config.Producer.Retry.Max = retryMax
	config.Producer.Return.Successes = successes
	config.Producer.Timeout = timeout
	config.Producer.Idempotent = cfg.Kafka.Publisher.Idempotent
	config.Net.MaxOpenRequests = maxOpenRequests
	config.Version = kafkaVersion
	config.Producer.Partitioner = cfg.Kafka.Publisher.Partitioner

	producer, err := sarama.NewSyncProducer([]string{broker}, config)
	if err != nil {
		return nil, err
	}

	return &publisher{
		broker:   broker,
		timeout:  cfg.Kafka.Publisher.Timeout,
		producer: producer,
		logger:   logger,
	}, nil
}
func (p *publisher) Close() error {
	return p.producer.Close()
}
func (p *publisher) Publish(ctx context.Context, opts ...event.PublishOption) {
	if len(opts) == 0 {
		p.logger.Error(ctx, "no options provided")
		return
	}

	config := &event.PublishConfig{}
	for _, opt := range opts {
		opt(config)
	}

	topic := config.Topic.String()
	bytes, err := json.Marshal(config.Payload)
	if err != nil {
		p.logger.Error(ctx, fmt.Sprintf("failed to marshal message for topic %s: %s", topic, err))
		return
	}

	saramaMsg := &sarama.ProducerMessage{
		Topic:     topic,
		Value:     sarama.ByteEncoder(bytes),
		Timestamp: time.Now().UTC(),
	}

	if config.MessageKey != "" {
		saramaMsg.Key = sarama.StringEncoder(config.MessageKey)
	}

	if len(config.Headers) > 0 {
		saramaMsg.Headers = config.Headers
	}

	partition, offset, err := p.producer.SendMessage(saramaMsg)
	if err != nil {
		p.logger.Error(ctx, fmt.Sprintf("failed to send message to topic %s: %s", topic, err))
		return
	}

	p.logger.Info(ctx, fmt.Sprintf("message sent to topic %s, partition %d, offset %d", topic, partition, offset))
}
