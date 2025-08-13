package sqs

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/andryhardiyanto/go-event"
	goLogger "github.com/andryhardiyanto/go-logger"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

type publisher struct {
	client    *sqs.Client
	logger    goLogger.Logger
	queueURLs map[string]string // cache: topic → queueURL
	useFIFO   bool
}

func NewPublisher(opts ...event.EventOption) event.Publisher {
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

	return &publisher{
		client:    cfg.Sqs.Client,
		logger:    cfg.Logger,
		queueURLs: make(map[string]string),
		useFIFO:   cfg.Sqs.UseFIFO,
	}
}

func (p *publisher) getOrCreateQueueURL(ctx context.Context, topic event.Topic) (string, error) {
	if url, ok := p.queueURLs[topic.String()]; ok {
		return url, nil
	}

	queueName := string(topic)
	if p.useFIFO && !strings.HasSuffix(queueName, ".fifo") {
		queueName += ".fifo"
	}

	// Cek queue
	getOut, err := p.client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
		QueueName: aws.String(queueName),
	})
	if err == nil && getOut.QueueUrl != nil {
		p.queueURLs[topic.String()] = *getOut.QueueUrl
		return *getOut.QueueUrl, nil
	}

	// Buat queue kalau belum ada
	createInput := &sqs.CreateQueueInput{
		QueueName: aws.String(queueName),
	}
	if p.useFIFO {
		createInput.Attributes = map[string]string{
			"FifoQueue":                 "true",
			"ContentBasedDeduplication": "true",
		}
	}

	createOut, err := p.client.CreateQueue(ctx, createInput)
	if err != nil {
		return "", fmt.Errorf("failed to create queue: %w", err)
	}

	p.queueURLs[topic.String()] = *createOut.QueueUrl
	return *createOut.QueueUrl, nil
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

	body, err := json.Marshal(config.Payload)
	if err != nil {
		p.logger.Error(ctx, fmt.Sprintf("failed to marshal payload: %v", err))
		return
	}

	queueURL, err := p.getOrCreateQueueURL(ctx, config.Topic)
	if err != nil {
		p.logger.Error(ctx, err.Error())
		return
	}

	input := &sqs.SendMessageInput{
		QueueUrl:    aws.String(queueURL),
		MessageBody: aws.String(string(body)),
	}

	if config.Sqs.UseFifo {
		groupID := config.Sqs.GroupID
		input.MessageGroupId = aws.String(groupID)
	}

	_, err = p.client.SendMessage(ctx, input)
	if err != nil {
		p.logger.Error(ctx, fmt.Sprintf("failed to send message to %v: %v", queueURL, err))
		return
	}

	p.logger.Info(ctx, fmt.Sprintf("successfully sent message to queue %s", queueURL))
}

func (p *publisher) Close() error {
	return nil
}
