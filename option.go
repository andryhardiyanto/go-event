package event

import (
	"time"

	"github.com/IBM/sarama"
	goLogger "github.com/andryhardiyanto/go-logger"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

type PublishOption func(*PublishConfig)

type PublishConfig struct {
	Topic      Topic
	Payload    any
	Sqs        PublishConfigSqs
	MessageKey string
	Headers    []sarama.RecordHeader
}

type PublishConfigSqs struct {
	GroupID string
}

func WithPublishHeaders(headers []sarama.RecordHeader) PublishOption {
	return func(c *PublishConfig) {
		c.Headers = headers
	}
}

func WithPublishKey(msgKey string) PublishOption {
	return func(c *PublishConfig) {
		c.MessageKey = msgKey
	}
}
func WithPublishTopic(topic Topic) PublishOption {
	return func(c *PublishConfig) {
		c.Topic = topic
	}
}

func WithPublishPayload(payload any) PublishOption {
	return func(c *PublishConfig) {
		c.Payload = payload
	}
}

func WithPublisherSqsGroupID(groupID string) PublishOption {
	return func(c *PublishConfig) {
		c.Sqs.GroupID = groupID
	}
}

type SubscribeOption func(*SubscribeOptionConfig)

type SubscribeOptionConfig struct {
	Topic   Topic
	Handler SubscriberHandler
	Kafka   SubscribeOptionConfigKafka
}

type SubscribeOptionConfigKafka struct {
	GroupID           Group
	RebalanceStrategy sarama.BalanceStrategy
}

func WithSubscribeOptionRebalanceStrategyKafka(strategy sarama.BalanceStrategy) SubscribeOption {
	return func(c *SubscribeOptionConfig) {
		c.Kafka.RebalanceStrategy = strategy
	}
}

func WithSubscribeOptionGroupIDKafka(groupID Group) SubscribeOption {
	return func(c *SubscribeOptionConfig) {
		c.Kafka.GroupID = groupID
	}
}

func WithSubscribeOptionHandler(handler SubscriberHandler) SubscribeOption {
	return func(c *SubscribeOptionConfig) {
		c.Handler = handler
	}
}

func WithSubscribeOptionTopic(topic Topic) SubscribeOption {
	return func(c *SubscribeOptionConfig) {
		c.Topic = topic
	}
}

type EventOption func(*EventConfig)

type EventConfig struct {
	Logger goLogger.Logger
	Kafka  Kafka
	Sqs    Sqs
}
type Kafka struct {
	Publisher  PublisherConfigKafka
	Subscriber SubscriberConfigKafka
	Version    *sarama.KafkaVersion
}
type Sqs struct {
	Client                                          *sqs.Client
	UseFIFO                                         bool
	MaxReceiveCount                                 int
	UseRedrivePermission                            bool
	UseDlq                                          bool
	QueueAttributeNameReceiveMessageWaitTimeSeconds int
	QueueAttributeNameVisibilityTimeout             int
	Publisher                                       PublisherConfigSqs
	Subscriber                                      SubscriberConfigSqs
}

type SubscriberConfigSqs struct {
	Timeout             time.Duration
	MaxNumberOfMessages int
	WaitTimeSeconds     int
}

type PublisherConfigSqs struct {
	Client  *sqs.Client
	Timeout time.Duration // default 5s
}
type SubscriberConfigKafka struct {
	Brokers []string
	Timeout time.Duration
}

type PublisherConfigKafka struct {
	Broker          string
	Timeout         time.Duration // default 5s
	Successes       bool
	RetryMax        int
	RequiredAcks    sarama.RequiredAcks
	Idempotent      bool
	MaxOpenRequests int
	Partitioner     sarama.PartitionerConstructor
	NumPartitions   int
}

func WithKafkaVersion(version sarama.KafkaVersion) EventOption {
	return func(cfg *EventConfig) {
		cfg.Kafka.Version = &version
	}
}

func WithKafkaPublisherIdempotent(idempotent bool) EventOption {
	return func(cfg *EventConfig) {
		cfg.Kafka.Publisher.Idempotent = idempotent
	}
}

func WithKafkaPublisherMaxOpenRequests(maxOpenRequests int) EventOption {
	return func(cfg *EventConfig) {
		cfg.Kafka.Publisher.MaxOpenRequests = maxOpenRequests
	}
}

func WithLogger(logger goLogger.Logger) EventOption {
	return func(cfg *EventConfig) {
		cfg.Logger = logger
	}
}

// Kafka
func WithKafkaPublisher(p PublisherConfigKafka) EventOption {
	return func(cfg *EventConfig) {
		cfg.Kafka.Publisher = p
	}
}

func WithKafkaPublisherPartitioner(partitioner sarama.PartitionerConstructor) EventOption {
	return func(cfg *EventConfig) {
		cfg.Kafka.Publisher.Partitioner = partitioner
	}
}

func WithKafkaPublisherBroker(broker string) EventOption {
	return func(cfg *EventConfig) {
		cfg.Kafka.Publisher.Broker = broker
	}
}

func WithKafkaPublisherTimeout(timeout time.Duration) EventOption {
	return func(cfg *EventConfig) {
		cfg.Kafka.Publisher.Timeout = timeout
	}
}

func WithKafkaPublisherSuccesses(success bool) EventOption {
	return func(cfg *EventConfig) {
		cfg.Kafka.Publisher.Successes = success
	}
}

func WithKafkaPublisherRetryMax(retry int) EventOption {
	return func(cfg *EventConfig) {
		cfg.Kafka.Publisher.RetryMax = retry
	}
}

func WithKafkaPublisherRequiredAcks(acks sarama.RequiredAcks) EventOption {
	return func(cfg *EventConfig) {
		cfg.Kafka.Publisher.RequiredAcks = acks
	}
}

func WithKafkaSubscriber(s SubscriberConfigKafka) EventOption {
	return func(cfg *EventConfig) {
		cfg.Kafka.Subscriber = s
	}
}

func WithKafkaSubscriberBrokers(brokers []string) EventOption {
	return func(cfg *EventConfig) {
		cfg.Kafka.Subscriber.Brokers = brokers
	}
}

func WithKafkaSubscriberTimeout(timeout time.Duration) EventOption {
	return func(cfg *EventConfig) {
		cfg.Kafka.Subscriber.Timeout = timeout
	}
}

// SQS
func WithSqsClient(client *sqs.Client) EventOption {
	return func(cfg *EventConfig) {
		cfg.Sqs.Client = client
	}
}
func WithSqsClientByAwsConfig(config aws.Config) EventOption {
	return func(cfg *EventConfig) {
		cfg.Sqs.Client = sqs.NewFromConfig(config)
	}
}
func WithSqsFIFO(useFIFO bool) EventOption {
	return func(cfg *EventConfig) {
		cfg.Sqs.UseFIFO = useFIFO
	}
}

func WithSqsPublisher(p PublisherConfigSqs) EventOption {
	return func(cfg *EventConfig) {
		cfg.Sqs.Publisher = p
	}
}

func WithSqsPublisherClient(client *sqs.Client) EventOption {
	return func(cfg *EventConfig) {
		cfg.Sqs.Publisher.Client = client
	}
}

func WithSqsPublisherTimeout(timeout time.Duration) EventOption {
	return func(cfg *EventConfig) {
		cfg.Sqs.Publisher.Timeout = timeout
	}
}

func WithSqsSubscriber(s SubscriberConfigSqs) EventOption {
	return func(cfg *EventConfig) {
		cfg.Sqs.Subscriber = s
	}
}

func WithSqsSubscriberTimeout(timeout time.Duration) EventOption {
	return func(cfg *EventConfig) {
		cfg.Sqs.Subscriber.Timeout = timeout
	}
}

func WithSqsMaxReceiveCount(maxReceiveCount int) EventOption {
	return func(cfg *EventConfig) {
		cfg.Sqs.MaxReceiveCount = maxReceiveCount
	}
}

func WithSqsUseRedrivePermission(useRedrivePermission bool) EventOption {
	return func(cfg *EventConfig) {
		cfg.Sqs.UseRedrivePermission = useRedrivePermission
	}
}

func WithSqsUseDlq(useDlq bool) EventOption {
	return func(cfg *EventConfig) {
		cfg.Sqs.UseDlq = useDlq
	}
}

func WithSqsQueueAttributeNameReceiveMessageWaitTimeSeconds(queueAttributeNameReceiveMessageWaitTimeSeconds int) EventOption {
	return func(cfg *EventConfig) {
		cfg.Sqs.QueueAttributeNameReceiveMessageWaitTimeSeconds = queueAttributeNameReceiveMessageWaitTimeSeconds
	}
}

func WithSqsQueueAttributeNameVisibilityTimeout(queueAttributeNameVisibilityTimeout int) EventOption {
	return func(cfg *EventConfig) {
		cfg.Sqs.QueueAttributeNameVisibilityTimeout = queueAttributeNameVisibilityTimeout
	}
}

func WithSqsSubscriberMaxNumberOfMessages(maxNumberOfMessages int) EventOption {
	return func(cfg *EventConfig) {
		cfg.Sqs.Subscriber.MaxNumberOfMessages = maxNumberOfMessages
	}
}

func WithSqsSubscriberWaitTimeSeconds(waitTimeSeconds int) EventOption {
	return func(cfg *EventConfig) {
		cfg.Sqs.Subscriber.WaitTimeSeconds = waitTimeSeconds
	}
}
