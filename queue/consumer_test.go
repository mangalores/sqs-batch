package queue

import (
	"context"
	"errors"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestConsumer_Start(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	messages := []*sqs.Message{
		{MessageId: aws.String("foo")},
		{MessageId: aws.String("bar")},
		{MessageId: aws.String("baz")},
	}

	client := &MockClient{cancel: cancel, messages: [][]*sqs.Message{{messages[0], messages[1]}, {messages[2]}}}
	handler := &MockBatchHandler{}

	consumer := Consumer{client: client, maxNumberOfMessages: 10}
	consumer.Start(ctx, handler)

	require.Len(t, handler.received, 3)
	actual := []*sqs.Message{}
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

	messages := []*sqs.Message{
		{MessageId: aws.String("foo"), ReceiptHandle: aws.String("bar")},
	}

	_, cancel := context.WithCancel(context.Background())
	client := &MockClient{cancel: cancel, messages: [][]*sqs.Message{{messages[0]}}, deleteErr: expectedClientErr}
	handler := &MockBatchHandler{handleErr: expectedHandleErr}
	hook := &MockLogHook{}
	logrus.AddHook(hook)

	consumer := Consumer{client: client, maxNumberOfMessages: 10, waitOnError: 0}
	consumer.consume(handler, messages)

	require.Len(t, handler.received, 1)
	require.Len(t, client.deletedMessages, 1)
	assert.Equal(t, messages[0], handler.received[0])
	assert.Equal(t, client.deletedMessages[0], messages[0].ReceiptHandle)
	assert.Equal(t, expectedHandleErr.Error(), hook.messages[0].Message)
	assert.Equal(t, logrus.ErrorLevel, hook.messages[0].Level)
	assert.Equal(t, expectedClientErr.Error(), hook.messages[1].Message)
	assert.Equal(t, logrus.ErrorLevel, hook.messages[1].Level)
}
