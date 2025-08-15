package sqs

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	goLogger "github.com/andryhardiyanto/go-logger"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"golang.org/x/sync/singleflight"
)

type QueueProvisioner struct {
	Client                                          *sqs.Client
	Logger                                          goLogger.Logger
	useFIFO                                         bool
	maxReceiveCount                                 int
	useRedrivePermission                            bool
	useDlq                                          bool
	queueAttributeNameReceiveMessageWaitTimeSeconds int
	queueAttributeNameVisibilityTimeout             int
	singleFlight                                    singleflight.Group
}

func NewQueueProvisioner(cli *sqs.Client, logger goLogger.Logger, useFIFO bool, maxReceiveCount int, useRedrivePermission bool, useDlq bool, queueAttributeNameReceiveMessageWaitTimeSeconds int, queueAttributeNameVisibilityTimeout int) *QueueProvisioner {
	return &QueueProvisioner{Client: cli, Logger: logger, useFIFO: useFIFO, maxReceiveCount: maxReceiveCount, useRedrivePermission: useRedrivePermission, useDlq: useDlq, queueAttributeNameReceiveMessageWaitTimeSeconds: queueAttributeNameReceiveMessageWaitTimeSeconds, queueAttributeNameVisibilityTimeout: queueAttributeNameVisibilityTimeout}
}

func (qp *QueueProvisioner) EnsureQueue(ctx context.Context, name string) (queueURL, queueARN string, err error) {
	if out, err := qp.Client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(name)}); err == nil && out.QueueUrl != nil {
		queueURL = aws.ToString(out.QueueUrl)
		arn, err2 := qp.GetQueueARN(ctx, queueURL)
		return queueURL, arn, err2
	}

	attrs := map[string]string{}
	if qp.queueAttributeNameReceiveMessageWaitTimeSeconds > 0 {
		attrs[string(types.QueueAttributeNameReceiveMessageWaitTimeSeconds)] = fmtInt(qp.queueAttributeNameReceiveMessageWaitTimeSeconds)
	}
	if qp.queueAttributeNameVisibilityTimeout > 0 {
		attrs[string(types.QueueAttributeNameVisibilityTimeout)] = fmtInt(qp.queueAttributeNameVisibilityTimeout)
	}
	if qp.useFIFO {
		attrs[string(types.QueueAttributeNameFifoQueue)] = "true"
		attrs["ContentBasedDeduplication"] = "true"
	}
	createOut, err := qp.Client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName:  aws.String(name),
		Attributes: attrs,
	})
	if err != nil {
		if out2, err2 := qp.Client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(name)}); err2 == nil && out2.QueueUrl != nil {
			queueURL = aws.ToString(out2.QueueUrl)
			arn, err3 := qp.GetQueueARN(ctx, queueURL)
			return queueURL, arn, err3
		}
		return "", "", fmt.Errorf("create queue %s: %w", name, err)
	}
	queueURL = aws.ToString(createOut.QueueUrl)
	arn, err := qp.GetQueueARN(ctx, queueURL)
	return queueURL, arn, err
}

func (qp *QueueProvisioner) GetQueueARN(ctx context.Context, queueURL string) (string, error) {
	attrOut, err := qp.Client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(queueURL),
		AttributeNames: []types.QueueAttributeName{
			types.QueueAttributeNameQueueArn,
			types.QueueAttributeNameRedrivePolicy,
		},
	})
	if err != nil {
		return "", fmt.Errorf("get attributes: %w", err)
	}
	return attrOut.Attributes[string(types.QueueAttributeNameQueueArn)], nil
}

func (qp *QueueProvisioner) EnsureMainAndDLQ(ctx context.Context, baseName string,
) (mainURL, mainARN, dlqURL, dlqARN string, err error) {

	mainName := baseName
	if qp.useFIFO && !strings.HasSuffix(mainName, ".fifo") {
		mainName += ".fifo"
	}

	mainURL, mainARN, err = qp.EnsureQueue(ctx, mainName)
	if err != nil {
		return "", "", "", "", fmt.Errorf("ensure main: %w", err)
	}

	if !qp.useDlq {
		_ = qp.clearRedrivePolicy(ctx, mainURL)
		return mainURL, mainARN, "", "", nil
	}

	dlqName := strings.TrimSuffix(mainName, ".fifo") + "-dlq"
	if qp.useFIFO {
		dlqName += ".fifo"
	}

	dlqURL, dlqARN, err = qp.EnsureQueue(ctx, dlqName)
	if err != nil {
		return "", "", "", "", fmt.Errorf("ensure dlq: %w", err)
	}

	if err := qp.ensureRedrivePolicy(ctx, mainURL, dlqARN, qp.maxReceiveCount); err != nil {
		return "", "", "", "", err
	}

	if qp.useRedrivePermission {
		if err := qp.ensureRedriveAllowPolicy(ctx, dlqURL, mainARN); err != nil {
			return "", "", "", "", err
		}
	}

	return mainURL, mainARN, dlqURL, dlqARN, nil
}

func (qp *QueueProvisioner) ensureRedrivePolicy(ctx context.Context, mainURL, dlqARN string, mrc int) error {
	cur, err := qp.Client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(mainURL),
		AttributeNames: []types.QueueAttributeName{
			types.QueueAttributeNameRedrivePolicy,
		},
	})
	if err != nil {
		return fmt.Errorf("get redrive policy: %w", err)
	}
	need := true
	if rp := cur.Attributes[string(types.QueueAttributeNameRedrivePolicy)]; rp != "" {
		var curM map[string]string
		if err := json.Unmarshal([]byte(rp), &curM); err != nil {
			return fmt.Errorf("unmarshal redrive policy: %w", err)
		}
		if curM["deadLetterTargetArn"] == dlqARN && curM["maxReceiveCount"] == fmtInt(mrc) {
			need = false
		}
	}
	if !need {
		return nil
	}
	raw, _ := json.Marshal(map[string]string{
		"deadLetterTargetArn": dlqARN,
		"maxReceiveCount":     fmtInt(mrc),
	})
	if _, err := qp.Client.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{
		QueueUrl: aws.String(mainURL),
		Attributes: map[string]string{
			"RedrivePolicy": string(raw),
		},
	}); err != nil {
		return fmt.Errorf("set redrive policy: %w", err)
	}
	return nil
}

func (qp *QueueProvisioner) clearRedrivePolicy(ctx context.Context, mainURL string) error {
	cur, _ := qp.Client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(mainURL),
		AttributeNames: []types.QueueAttributeName{
			types.QueueAttributeNameRedrivePolicy,
		},
	})
	if rp := cur.Attributes[string(types.QueueAttributeNameRedrivePolicy)]; rp != "" {
		_, _ = qp.Client.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{
			QueueUrl:   aws.String(mainURL),
			Attributes: map[string]string{"RedrivePolicy": ""},
		})
	}
	return nil
}

func (qp *QueueProvisioner) ensureRedriveAllowPolicy(ctx context.Context, dlqURL, mainARN string) error {
	want := map[string]interface{}{
		"redrivePermission": "byQueue",
		"sourceQueueArns":   []string{mainARN},
	}
	wantJSON, _ := json.Marshal(want)
	cur, _ := qp.Client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(dlqURL),
		AttributeNames: []types.QueueAttributeName{
			types.QueueAttributeNameRedriveAllowPolicy,
		},
	})
	if raw := cur.Attributes[string(types.QueueAttributeNameRedriveAllowPolicy)]; raw == string(wantJSON) && raw != "" {
		return nil
	}
	if _, err := qp.Client.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{
		QueueUrl: aws.String(dlqURL),
		Attributes: map[string]string{
			"RedriveAllowPolicy": string(wantJSON),
		},
	}); err != nil {
		return fmt.Errorf("set redrive allow policy: %w", err)
	}
	return nil
}

func (qp *QueueProvisioner) GetOrCreateQueueURL(
	ctx context.Context,
	key string,
) (string, error) {
	cache := SharedURLCache()
	if u, ok := cache.Get(key); ok {
		return u, nil
	}

	v, err, _ := qp.singleFlight.Do(key, func() (any, error) {
		if u2, ok2 := cache.Get(key); ok2 {
			return u2, nil
		}
		mainURL, _, _, _, err := qp.EnsureMainAndDLQ(ctx, key)
		if err != nil {
			return "", err
		}
		cache.Set(key, mainURL)
		return mainURL, nil
	})
	if err != nil {
		return "", err
	}

	return v.(string), nil
}

func fmtInt(v int) string { return fmt.Sprintf("%d", v) }
