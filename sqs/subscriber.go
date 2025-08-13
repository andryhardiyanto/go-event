package sqs

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/andryhardiyanto/go-event"
	goLogger "github.com/andryhardiyanto/go-logger"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqsTypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type subscriber struct {
	client       *sqs.Client
	timeout      time.Duration
	logger       goLogger.Logger
	handlers     map[string]event.SubscriberHandler
	queues       map[string]string
	cancel       context.CancelFunc
	wg           sync.WaitGroup
	useFIFOQueue bool
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

	return &subscriber{
		client:       cfg.Sqs.Client,
		timeout:      cfg.Sqs.Subscriber.Timeout,
		logger:       cfg.Logger,
		handlers:     make(map[string]event.SubscriberHandler),
		queues:       make(map[string]string),
		useFIFOQueue: cfg.Sqs.UseFIFO,
	}
}

// getOrCreateQueueURL memastikan semua instance pakai queue yang sama
func (s *subscriber) getOrCreateQueueURL(ctx context.Context, topic string) (string, error) {
	queueName := topic
	if s.useFIFOQueue && !strings.HasSuffix(queueName, ".fifo") {
		queueName += ".fifo"
	}

	// Cek apakah queue sudah ada
	getQueueOut, err := s.client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
		QueueName: aws.String(queueName),
	})
	if err == nil && getQueueOut.QueueUrl != nil {
		return *getQueueOut.QueueUrl, nil
	}

	var notFound *sqsTypes.QueueDoesNotExist
	if errors.As(err, &notFound) {
		// Buat queue baru
		createInput := &sqs.CreateQueueInput{
			QueueName: aws.String(queueName),
			Attributes: map[string]string{
				"VisibilityTimeout": fmt.Sprintf("%d", int(s.timeout.Seconds())),
			},
		}
		if s.useFIFOQueue {
			createInput.Attributes["FifoQueue"] = "true"
			createInput.Attributes["ContentBasedDeduplication"] = "true"
		}

		createOut, createErr := s.client.CreateQueue(ctx, createInput)
		if createErr != nil {
			return "", fmt.Errorf("failed to create queue: %w", createErr)
		}
		return *createOut.QueueUrl, nil
	}

	return "", fmt.Errorf("failed to get queue url: %w", err)
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

	queueURL, err := s.getOrCreateQueueURL(context.Background(), topicStr)
	if err != nil {
		s.logger.Panic(context.Background(), err.Error())
	}

	s.queues[topicStr] = queueURL
}

func (s *subscriber) Start(parent context.Context) error {
	if len(s.handlers) == 0 {
		return fmt.Errorf("no subscriptions registered")
	}

	ctx, cancel := context.WithCancel(parent)
	s.cancel = cancel

	for topic, handler := range s.handlers {
		queueURL := s.queues[topic]
		s.wg.Add(1)
		go func(topic, queueURL string, handler event.SubscriberHandler) {
			defer s.wg.Done()

			for {
				select {
				case <-ctx.Done():
					return
				default:
					output, err := s.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
						QueueUrl:            aws.String(queueURL),
						MaxNumberOfMessages: 10,
						WaitTimeSeconds:     10,
						// Tidak perlu set VisibilityTimeout lagi, pakai default dari queue
					})
					if err != nil {
						if ctx.Err() != nil || errors.Is(err, context.Canceled) {
							s.logger.Info(ctx, fmt.Sprintf("stopping consumer for topic %s: context cancelled", topic))
							return
						}
						s.logger.Error(ctx, fmt.Sprintf("receive error on topic %s: %v", topic, err))
						time.Sleep(5 * time.Second) // retry sederhana
						continue
					}

					for _, msg := range output.Messages {
						if ctx.Err() != nil {
							s.logger.Info(ctx, fmt.Sprintf("stopping message processing for topic %s: context cancelled", topic))
							return
						}

						s.logger.Info(ctx, fmt.Sprintf("received message topic=%s messageID=%s", topic, aws.ToString(msg.MessageId)))

						processCtx, cancel := context.WithTimeout(ctx, s.timeout)
						err := handler(processCtx, msg)
						cancel()

						if err != nil {
							if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
								s.logger.Info(ctx, fmt.Sprintf("message processing cancelled for topic %s: %v", topic, err))
								return
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
								s.logger.Info(ctx, fmt.Sprintf("stopping message deletion for topic %s: context cancelled", topic))
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
