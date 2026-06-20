package awssink

import (
	"context"
	"errors"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// sqsPublisher delivers records to an SQS queue via SendMessage.
type sqsPublisher struct {
	client   *sqs.Client
	queueURL string
	fifo     bool
}

// NewSQSPublisher builds a real SQS Publisher from an aws.Config. queueURL is
// required; fifo must be set for a .fifo queue (so a MessageGroupId and
// MessageDeduplicationId are sent — both required there and rejected otherwise).
// It performs no network I/O.
func NewSQSPublisher(cfg aws.Config, queueURL string, fifo bool) (Publisher, error) {
	q := strings.TrimSpace(queueURL)
	if q == "" {
		return nil, errors.New("awssink: sqs queue_url is required")
	}
	return &sqsPublisher{client: sqs.NewFromConfig(cfg), queueURL: q, fifo: fifo}, nil
}

func (p *sqsPublisher) Publish(ctx context.Context, msg Message) error {
	in := &sqs.SendMessageInput{
		QueueUrl:    aws.String(p.queueURL),
		MessageBody: aws.String(string(msg.Body)),
	}
	if p.fifo {
		in.MessageGroupId = aws.String(msg.GroupID)
		in.MessageDeduplicationId = aws.String(msg.DedupID)
	}
	_, err := p.client.SendMessage(ctx, in)
	return err
}

// Reachable confirms the queue exists and is accessible (a read-only call).
func (p *sqsPublisher) Reachable(ctx context.Context) error {
	_, err := p.client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(p.queueURL),
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	return err
}

// Target returns the queue URL (a resource identifier, not a secret) for logs.
func (p *sqsPublisher) Target() string { return p.queueURL }

func (p *sqsPublisher) Close() error { return nil }
