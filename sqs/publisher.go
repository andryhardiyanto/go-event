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
	client           *sqs.Client
	logger           goLogger.Logger
	useFIFO          bool
	queueProvisioner *QueueProvisioner
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

	if cfg.Sqs.QueueAttributeNameReceiveMessageWaitTimeSeconds == 0 {
		cfg.Sqs.QueueAttributeNameReceiveMessageWaitTimeSeconds = 20
	}
	if cfg.Sqs.QueueAttributeNameVisibilityTimeout == 0 {
		cfg.Sqs.QueueAttributeNameVisibilityTimeout = 60
	}
	if cfg.Sqs.MaxReceiveCount == 0 {
		cfg.Sqs.MaxReceiveCount = 1
	}

	return &publisher{
		client:  cfg.Sqs.Client,
		logger:  cfg.Logger,
		useFIFO: cfg.Sqs.UseFIFO,
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
	}
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

	queueURL, err := p.queueProvisioner.GetOrCreateQueueURL(ctx, config.Topic.String())
	if err != nil {
		p.logger.Error(ctx, err.Error())
		return
	}

	input := &sqs.SendMessageInput{
		QueueUrl:    aws.String(queueURL),
		MessageBody: aws.String(string(body)),
	}

	if p.useFIFO {
		groupID := strings.TrimSpace(config.Sqs.GroupID)
		if groupID == "" {
			p.logger.Error(ctx, "FIFO queue requires a non-empty MessageGroupId")
			return
		}
		input.MessageGroupId = aws.String(groupID)
	}

	if _, err := p.client.SendMessage(ctx, input); err != nil {
		p.logger.Error(ctx, fmt.Sprintf("failed to send message to %v: %v", queueURL, err))
		return
	}

	p.logger.Info(ctx, fmt.Sprintf("successfully sent message to queue %s", queueURL))
}

func (p *publisher) Close() error { return nil }
