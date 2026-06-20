package awssink

import (
	"context"
	"errors"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
)

// snsPublisher delivers records to an SNS topic via Publish.
type snsPublisher struct {
	client   *sns.Client
	topicARN string
	fifo     bool
}

// NewSNSPublisher builds a real SNS Publisher from an aws.Config. topicARN is
// required; fifo must be set for a .fifo topic (so a MessageGroupId and
// MessageDeduplicationId are sent). It performs no network I/O.
func NewSNSPublisher(cfg aws.Config, topicARN string, fifo bool) (Publisher, error) {
	t := strings.TrimSpace(topicARN)
	if t == "" {
		return nil, errors.New("awssink: sns topic_arn is required")
	}
	return &snsPublisher{client: sns.NewFromConfig(cfg), topicARN: t, fifo: fifo}, nil
}

func (p *snsPublisher) Publish(ctx context.Context, msg Message) error {
	in := &sns.PublishInput{
		TopicArn: aws.String(p.topicARN),
		Message:  aws.String(string(msg.Body)),
	}
	if p.fifo {
		in.MessageGroupId = aws.String(msg.GroupID)
		in.MessageDeduplicationId = aws.String(msg.DedupID)
	}
	_, err := p.client.Publish(ctx, in)
	return err
}

// Reachable confirms the topic exists and is accessible (a read-only call).
func (p *snsPublisher) Reachable(ctx context.Context) error {
	_, err := p.client.GetTopicAttributes(ctx, &sns.GetTopicAttributesInput{
		TopicArn: aws.String(p.topicARN),
	})
	return err
}

// Target returns the topic ARN (a resource identifier, not a secret) for logs.
func (p *snsPublisher) Target() string { return p.topicARN }

func (p *snsPublisher) Close() error { return nil }
