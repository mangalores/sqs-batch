package queue

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/sirupsen/logrus"
	"sync"
)

var mx = sync.RWMutex{}

type MockClient struct {
	cancel           context.CancelFunc
	messages         [][]types.Message
	queueUrl         string
	queueUrlErr      error
	deletedMessages  []*string
	deleteBatchSizes []int
	deleteErr        error
}

func (m *MockClient) ReceiveMessage(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	var messages []types.Message
	if len(m.messages) == 0 {
		m.cancel()
		messages = []types.Message{}
	} else {
		messages = m.messages[0]
		m.messages = m.messages[1:]
	}

	return &sqs.ReceiveMessageOutput{Messages: messages}, nil
}

func (m *MockClient) DeleteMessageBatch(ctx context.Context, input *sqs.DeleteMessageBatchInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageBatchOutput, error) {
	mx.Lock()
	defer mx.Unlock()

	for _, entry := range input.Entries {
		m.deletedMessages = append(m.deletedMessages, entry.Id)
	}

	m.deleteBatchSizes = append(m.deleteBatchSizes, len(input.Entries))

	return &sqs.DeleteMessageBatchOutput{}, m.deleteErr
}

func (m *MockClient) GetQueueUrl(context.Context, *sqs.GetQueueUrlInput, ...func(o *sqs.Options)) (*sqs.GetQueueUrlOutput, error) {
	return &sqs.GetQueueUrlOutput{QueueUrl: aws.String(m.queueUrl)}, m.queueUrlErr
}

type MockBatchHandler struct {
	received  []types.Message
	handleErr error
}

func (h *MockBatchHandler) Handle(ctx context.Context, m []types.Message) error {
	h.received = append(h.received, m...)

	return h.handleErr
}

type MockSingleHandler struct {
	received  []types.Message
	handleErr error
	mx        sync.RWMutex
}

func (h *MockSingleHandler) Handle(ctx context.Context, m types.Message) error {
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
