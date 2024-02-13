package queue

import (
	"context"
	"errors"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
)

func TestWrap(t *testing.T) {
	single := &MockSingleHandler{mx: sync.RWMutex{}}
	handler := Wrap(single)

	assert.Equal(t, single, handler.handler)
}

func TestWrapperHandler_Handle(t *testing.T) {
	single := &MockSingleHandler{mx: sync.RWMutex{}}
	handler := WrapperHandler{handler: single, wg: &sync.WaitGroup{}}

	messages := []types.Message{
		{MessageId: aws.String("foo")},
		{MessageId: aws.String("bar")},
		{MessageId: aws.String("baz")},
	}

	err := handler.Handle(context.Background(), messages)

	require.Nil(t, err)
	require.Len(t, single.received, 3)
	actual := []types.Message{}
	for _, a := range single.received {
		for _, e := range messages {
			if e.MessageId == a.MessageId {
				actual = append(actual, a)
				break
			}
		}
	}
	assert.Len(t, actual, 3)
}

func TestWrapperHandler_HandleErr(t *testing.T) {
	messages := []types.Message{{MessageId: aws.String("foo")}}
	expectedErr := errors.New("foo bAz BaR")

	hook := &MockLogHook{}
	logrus.AddHook(hook)
	single := &MockSingleHandler{handleErr: expectedErr, mx: sync.RWMutex{}}
	handler := WrapperHandler{handler: single, wg: &sync.WaitGroup{}}

	err := handler.Handle(context.Background(), messages)
	require.Nil(t, err)
	require.Len(t, single.received, 1)
	require.Len(t, hook.messages, 1)

	assert.Equal(t, logrus.ErrorLevel, hook.messages[0].Level)
	assert.Equal(t, expectedErr.Error(), hook.messages[0].Message)

}
