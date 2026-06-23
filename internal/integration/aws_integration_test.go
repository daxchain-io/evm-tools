//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/daxchain-io/evm-tools/internal/awssink"
)

// awsTestConfig points the AWS SDK at LocalStack with throwaway static creds.
func awsTestConfig(t *testing.T) aws.Config {
	t.Helper()
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	cfg, err := awssink.LoadAWSConfig(context.Background(), awssink.ClientConfig{
		Region:      "us-east-1",
		EndpointURL: envOr("EVM_TEST_AWS_ENDPOINT", "http://localhost:4566"),
	})
	if err != nil {
		t.Fatalf("LoadAWSConfig: %v", err)
	}
	return cfg
}

// TestSQSSinkLive sends a record through the SQS publisher to a real (LocalStack)
// queue and receives it back.
func TestSQSSinkLive(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cfg := awsTestConfig(t)
	cl := sqs.NewFromConfig(cfg)

	cq, err := cl.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(uniqueName("evmtest-"))})
	if err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	queueURL := aws.ToString(cq.QueueUrl)

	pub, err := awssink.NewSQSPublisher(cfg, queueURL, false)
	if err != nil {
		t.Fatalf("NewSQSPublisher: %v", err)
	}
	defer func() { _ = pub.Close() }()
	_, raw := sampleRecord(t, "0xsqs")
	if err := pub.Publish(ctx, awssink.Message{Body: raw}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	out, err := cl.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl: aws.String(queueURL), MaxNumberOfMessages: 1, WaitTimeSeconds: 5,
	})
	if err != nil {
		t.Fatalf("ReceiveMessage: %v", err)
	}
	if len(out.Messages) != 1 || aws.ToString(out.Messages[0].Body) != string(raw) {
		t.Fatalf("sqs message mismatch: got %d messages", len(out.Messages))
	}
}

// TestSNSSinkLive publishes a record through the SNS publisher to a real
// (LocalStack) topic subscribed by an SQS queue (raw delivery), then receives it.
func TestSNSSinkLive(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cfg := awsTestConfig(t)
	snsCl := sns.NewFromConfig(cfg)
	sqsCl := sqs.NewFromConfig(cfg)

	topic, err := snsCl.CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String(uniqueName("evmtest-"))})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	topicARN := aws.ToString(topic.TopicArn)

	cq, err := sqsCl.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(uniqueName("evmtest-sub-"))})
	if err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	queueURL := aws.ToString(cq.QueueUrl)
	attrs, err := sqsCl.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(queueURL), AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	if err != nil {
		t.Fatalf("GetQueueAttributes: %v", err)
	}
	queueARN := attrs.Attributes[string(sqstypes.QueueAttributeNameQueueArn)]

	if _, err := snsCl.Subscribe(ctx, &sns.SubscribeInput{
		TopicArn:              aws.String(topicARN),
		Protocol:              aws.String("sqs"),
		Endpoint:              aws.String(queueARN),
		Attributes:            map[string]string{"RawMessageDelivery": "true"},
		ReturnSubscriptionArn: true,
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	pub, err := awssink.NewSNSPublisher(cfg, topicARN, false)
	if err != nil {
		t.Fatalf("NewSNSPublisher: %v", err)
	}
	defer func() { _ = pub.Close() }()
	_, raw := sampleRecord(t, "0xsns")
	if err := pub.Publish(ctx, awssink.Message{Body: raw}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// Poll the subscribed queue (SNS->SQS fan-out is async).
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		out, err := sqsCl.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl: aws.String(queueURL), MaxNumberOfMessages: 1, WaitTimeSeconds: 2,
		})
		if err != nil {
			t.Fatalf("ReceiveMessage: %v", err)
		}
		if len(out.Messages) == 1 {
			if got := aws.ToString(out.Messages[0].Body); got != string(raw) {
				t.Fatalf("sns->sqs body mismatch:\n got %s\nwant %s", got, raw)
			}
			return
		}
	}
	t.Fatal("SNS message did not arrive on the subscribed SQS queue within 20s")
}
