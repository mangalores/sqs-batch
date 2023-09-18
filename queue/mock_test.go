package queue

import (
	"context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/sirupsen/logrus"
	"sync"
)

type MockClient struct {
	cancel          context.CancelFunc
	messages        [][]*sqs.Message
	queueUrl        string
	queueUrlErr     error
	deletedMessages []*string
	deleteErr       error
}

func (m *MockClient) ReceiveMessage(*sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error) {
	var messages []*sqs.Message
	if len(m.messages) == 0 {
		m.cancel()
		messages = []*sqs.Message{}
	} else {
		messages = m.messages[0]
		m.messages = m.messages[1:]
	}

	return &sqs.ReceiveMessageOutput{Messages: messages}, nil
}

func (m *MockClient) DeleteMessage(input *sqs.DeleteMessageInput) (*sqs.DeleteMessageOutput, error) {
	m.deletedMessages = append(m.deletedMessages, input.ReceiptHandle)

	return &sqs.DeleteMessageOutput{}, m.deleteErr
}

func (m *MockClient) GetQueueUrl(_ *sqs.GetQueueUrlInput) (*sqs.GetQueueUrlOutput, error) {
	return &sqs.GetQueueUrlOutput{QueueUrl: aws.String(m.queueUrl)}, m.queueUrlErr
}

type MockBatchHandler struct {
	received  []*sqs.Message
	handleErr error
}

func (h *MockBatchHandler) Handle(m []*sqs.Message) error {
	h.received = append(h.received, m...)

	return h.handleErr
}

type MockSingleHandler struct {
	received  []*sqs.Message
	handleErr error
	mx        sync.RWMutex
}

func (h *MockSingleHandler) Handle(m *sqs.Message) error {
	h.mx.Lock()
	defer h.mx.Unlock()

	h.received = append(h.received, m)

	return h.handleErr
}

type MockLogHook struct {
	messages []*logrus.Entry
}

func (t *MockLogHook) Levels() []logrus.Level {
	return []logrus.Level{logrus.ErrorLevel, logrus.WarnLevel}

}
func (t *MockLogHook) Fire(entry *logrus.Entry) error {
	t.messages = append(t.messages, entry)
	return nil
}
