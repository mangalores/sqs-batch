package queue

import (
	"context"
	"errors"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNewSQSConsumerOK(t *testing.T) {
	client := &MockClient{
		queueUrl: "https://foo.bar/baz",
	}

	config := ConsumerConfig{}
	handler := &MockBatchHandler{}
	consumer, err := NewConsumer(config, client, handler)

	require.Nil(t, err)

	assert.IsType(t, &Consumer{}, consumer)
}

func TestNewSQSConsumerGetQueueUrlErr(t *testing.T) {
	expectedErr := errors.New("foo bar baz")
	client := &MockClient{
		queueUrl:    "https://foo.bar/baz",
		queueUrlErr: expectedErr,
	}

	config := ConsumerConfig{}
	handler := &MockBatchHandler{}
	consumer, err := NewConsumer(config, client, handler)

	require.NotNil(t, err)
	assert.Nil(t, consumer)
	assert.Equal(t, expectedErr, err)
}

func TestConsumer_Start(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	messages := []types.Message{
		{MessageId: aws.String("foo")},
		{MessageId: aws.String("bar")},
		{MessageId: aws.String("baz")},
	}

	client := &MockClient{cancel: cancel, messages: [][]types.Message{{messages[0], messages[1]}, {messages[2]}}}
	handler := &MockBatchHandler{}

	consumer := Consumer{client: client, maxNumberOfMessages: 10, handler: handler}
	consumer.Start(ctx)

	require.Len(t, handler.received, 3)
	actual := []types.Message{}
	for _, a := range handler.received {
		for _, e := range messages {
			if e.MessageId == a.MessageId {
				actual = append(actual, a)
				break
			}
		}
	}
	assert.Len(t, actual, 3)
}

func TestConsumer_ConsumeHandleErr(t *testing.T) {
	expectedHandleErr := errors.New("foo bar baz")
	expectedClientErr := errors.New("baz bar foo")

	messages := []types.Message{
		{MessageId: aws.String("foo"), ReceiptHandle: aws.String("bar")},
	}

	_, cancel := context.WithCancel(context.Background())
	client := &MockClient{cancel: cancel, messages: [][]types.Message{{messages[0]}}, deleteErr: expectedClientErr}
	handler := &MockBatchHandler{handleErr: expectedHandleErr}
	hook := &MockLogHook{}
	logrus.AddHook(hook)

	consumer := Consumer{client: client, maxNumberOfMessages: 10, waitOnError: 0, handler: handler}
	consumer.runBatch(context.Background())

	require.Len(t, handler.received, 1)
	require.Len(t, client.deletedMessages, 1)
	assert.Equal(t, messages[0], handler.received[0])
	assert.Equal(t, client.deletedMessages[0], messages[0].MessageId)
	assert.Equal(t, expectedHandleErr.Error(), hook.messages[0].Message)
	assert.Equal(t, logrus.ErrorLevel, hook.messages[0].Level)
	assert.Equal(t, expectedClientErr.Error(), hook.messages[1].Message)
	assert.Equal(t, logrus.ErrorLevel, hook.messages[1].Level)
}

func TestConsumer_ConsumeHandle(t *testing.T) {
	expectedHandleErr := errors.New("foo bar baz")
	expectedClientErr := errors.New("baz bar foo")

	messages := []types.Message{
		{MessageId: aws.String("foo1"), ReceiptHandle: aws.String("bar")},
		{MessageId: aws.String("foo2"), ReceiptHandle: aws.String("bar")},
		{MessageId: aws.String("foo3"), ReceiptHandle: aws.String("bar")},
		{MessageId: aws.String("foo4"), ReceiptHandle: aws.String("bar")},
		{MessageId: aws.String("foo5"), ReceiptHandle: aws.String("bar")},
		{MessageId: aws.String("foo6"), ReceiptHandle: aws.String("bar")},
		{MessageId: aws.String("foo7"), ReceiptHandle: aws.String("bar")},
		{MessageId: aws.String("foo8"), ReceiptHandle: aws.String("bar")},
		{MessageId: aws.String("foo9"), ReceiptHandle: aws.String("bar")},
		{MessageId: aws.String("foo10"), ReceiptHandle: aws.String("bar")},
		{MessageId: aws.String("foo11"), ReceiptHandle: aws.String("bar")},
		{MessageId: aws.String("foo12"), ReceiptHandle: aws.String("bar")},
	}

	_, cancel := context.WithCancel(context.Background())
	client := &MockClient{cancel: cancel, messages: [][]types.Message{{messages[0]}}, deleteErr: expectedClientErr}
	handler := &MockBatchHandler{handleErr: expectedHandleErr}
	hook := &MockLogHook{}
	logrus.AddHook(hook)

	consumer := Consumer{client: client, maxNumberOfMessages: 10, waitOnError: 0, handler: handler}
	consumer.dropMessages(context.Background(), messages)

	assert.Len(t, client.deletedMessages, 12)
	require.Len(t, client.deleteBatchSizes, 2)
	assert.LessOrEqual(t, client.deleteBatchSizes[0], 10)
	assert.LessOrEqual(t, client.deleteBatchSizes[1], 10)
}
