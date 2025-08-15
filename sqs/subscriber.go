package sqs

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/andryhardiyanto/go-event"
	goLogger "github.com/andryhardiyanto/go-logger"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

type subscriber struct {
	client              *sqs.Client
	timeout             time.Duration
	logger              goLogger.Logger
	handlers            map[string]event.SubscriberHandler
	cancel              context.CancelFunc
	wg                  sync.WaitGroup
	queueProvisioner    *QueueProvisioner
	maxNumberOfMessages int
	waitTimeSeconds     int
	random              *rand.Rand
}

func NewSubscriber(opts ...event.EventOption) event.Subscriber {
	if len(opts) == 0 {
		panic("no options provided")
	}

	cfg := &event.EventConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	if cfg.Sqs.Client == nil {
		panic("sqs client is nil")
	}

	if cfg.Logger == nil {
		panic("logger is nil")
	}

	if cfg.Sqs.Subscriber.Timeout == 0 {
		cfg.Sqs.Subscriber.Timeout = 5 * time.Second
	}

	if cfg.Sqs.QueueAttributeNameReceiveMessageWaitTimeSeconds == 0 {
		cfg.Sqs.QueueAttributeNameReceiveMessageWaitTimeSeconds = 20
	}
	if cfg.Sqs.QueueAttributeNameVisibilityTimeout == 0 {
		cfg.Sqs.QueueAttributeNameVisibilityTimeout = 60
	}
	if cfg.Sqs.MaxReceiveCount == 0 {
		cfg.Sqs.MaxReceiveCount = 1
	}
	if cfg.Sqs.Subscriber.MaxNumberOfMessages == 0 {
		cfg.Sqs.Subscriber.MaxNumberOfMessages = 1
	}
	if cfg.Sqs.Subscriber.WaitTimeSeconds == 0 {
		cfg.Sqs.Subscriber.WaitTimeSeconds = 20
	}

	return &subscriber{
		client:              cfg.Sqs.Client,
		timeout:             cfg.Sqs.Subscriber.Timeout,
		logger:              cfg.Logger,
		handlers:            make(map[string]event.SubscriberHandler),
		maxNumberOfMessages: cfg.Sqs.Subscriber.MaxNumberOfMessages,
		waitTimeSeconds:     cfg.Sqs.Subscriber.WaitTimeSeconds,
		queueProvisioner: NewQueueProvisioner(
			cfg.Sqs.Client,
			cfg.Logger,
			cfg.Sqs.UseFIFO,
			cfg.Sqs.MaxReceiveCount,
			cfg.Sqs.UseRedrivePermission,
			cfg.Sqs.UseDlq,
			cfg.Sqs.QueueAttributeNameReceiveMessageWaitTimeSeconds,
			cfg.Sqs.QueueAttributeNameVisibilityTimeout,
		),
		random: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (s *subscriber) Subscribe(opts ...event.SubscribeOption) {
	cfg := &event.SubscribeOptionConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	topicStr := cfg.Topic.String()

	if cfg.Handler == nil {
		s.logger.Panic(context.Background(), "handler is required")
	}

	if cfg.Topic.String() == "" {
		s.logger.Panic(context.Background(), "topic is required")
	}

	if _, ok := s.handlers[topicStr]; ok {
		s.logger.Panic(context.Background(), fmt.Sprintf("already subscribed to topic %s", topicStr))
	}
	s.handlers[topicStr] = cfg.Handler

	_, err := s.queueProvisioner.GetOrCreateQueueURL(context.Background(), topicStr)
	if err != nil {
		s.logger.Panic(context.Background(), err.Error())
	}

}
func (s *subscriber) backoffFullJitter(attempt int, base, capDelay time.Duration) time.Duration {
	d := base << attempt
	if d > capDelay {
		d = capDelay
	}
	if d <= 0 {
		return 0
	}

	return time.Duration(s.random.Int63n(int64(d)))
}
func (s *subscriber) Start(parent context.Context) error {
	if len(s.handlers) == 0 {
		return fmt.Errorf("no subscriptions registered")
	}

	ctx, cancel := context.WithCancel(parent)
	s.cancel = cancel

	for topic, handler := range s.handlers {
		queueURL, err := s.queueProvisioner.GetOrCreateQueueURL(ctx, topic)
		if err != nil {
			return err
		}

		s.wg.Add(1)
		go func(topic, queueURL string, handler event.SubscriberHandler) {
			defer s.wg.Done()
			errStreak := 0

			for {
				select {
				case <-ctx.Done():
					return
				default:
					output, err := s.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
						QueueUrl:            aws.String(queueURL),
						MaxNumberOfMessages: int32(s.maxNumberOfMessages),
						WaitTimeSeconds:     int32(s.waitTimeSeconds),
					})
					if err != nil {
						if ctx.Err() != nil || errors.Is(err, context.Canceled) {
							s.logger.Warn(ctx, fmt.Sprintf("stopping consumer for topic %s: context cancelled", topic))
							return
						}
						s.logger.Error(ctx, fmt.Sprintf("receive error on topic %s: %v", topic, err))
						waitTime := s.backoffFullJitter(errStreak, 500*time.Millisecond, 10*time.Second)
						errStreak++
						select {
						case <-ctx.Done():
							return
						case <-time.After(waitTime):
						}
						continue
					}
					errStreak = 0

					for _, msg := range output.Messages {
						if ctx.Err() != nil {
							s.logger.Warn(ctx, fmt.Sprintf("stopping message processing for topic %s: context cancelled", topic))
							return
						}

						s.logger.Info(ctx, fmt.Sprintf("received message topic=%s messageID=%s", topic, aws.ToString(msg.MessageId)))

						processCtx, msgCancel := context.WithTimeout(ctx, s.timeout)
						err := handler(processCtx, msg)
						msgCancel()

						if err != nil {
							if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
								s.logger.Warn(ctx, fmt.Sprintf("message processing cancelled for topic %s: %v", topic, err))
								continue
							}
							s.logger.Error(ctx, fmt.Sprintf("error handling message on topic %s: %v", topic, err))
							continue
						}

						_, err = s.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
							QueueUrl:      aws.String(queueURL),
							ReceiptHandle: msg.ReceiptHandle,
						})
						if err != nil {
							if ctx.Err() != nil || errors.Is(err, context.Canceled) {
								s.logger.Warn(ctx, fmt.Sprintf("stopping message deletion for topic %s: context cancelled", topic))
								return
							}
							s.logger.Error(ctx, fmt.Sprintf("delete error topic=%s: %v", topic, err))
						}
					}
				}
			}
		}(topic, queueURL, handler)
	}
	return nil
}

func (s *subscriber) Close() error {
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
	return nil
}
