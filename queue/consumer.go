package queue

import (
	"context"
	"github.com/sirupsen/logrus"
	"math"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

const maxMessagesPerRequest = 10
const parallelRequests = 10

type SQSReceiver interface {
	DeleteMessage(*sqs.DeleteMessageInput) (*sqs.DeleteMessageOutput, error)
	ReceiveMessage(*sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error)
}

type BatchHandler interface {
	Handle(msg []*sqs.Message) error
}

// Consumer struct
type Consumer struct {
	queueURL                 string
	maxNumberOfMessages      int64
	waitTimeSeconds          int64
	visibilityTimeoutSeconds int64
	waitOnError              time.Duration

	client SQSReceiver
}

// Start starts the polling and will continue polling till the application is forcibly stopped
func (c *Consumer) Start(ctx context.Context, h BatchHandler) {
	for {
		select {
		case <-ctx.Done():
			logrus.Debug("consumer: Stopping polling because a context kill signal was sent")
			return
		default:
			messages := c.pullMessages()
			numMessages := len(messages)
			if numMessages > 0 {
				logrus.Infof("consumer: Received %d messages", numMessages)
				c.consume(h, messages)
			}
		}
	}
}

// consume launches goroutine per received message and wait for all message to be processed
func (c *Consumer) consume(h BatchHandler, messages []*sqs.Message) {
	var err error

	err = h.Handle(messages)
	if err != nil {
		logrus.Error(err)
	}

	err = c.dropMessages(messages)
	if err != nil {
		logrus.Error(err)
	}
}

func (c *Consumer) pullMessages() []*sqs.Message {
	requests := c.generateReceiveRequests()

	semaphore := make(chan int, parallelRequests)
	defer close(semaphore)

	wg := &sync.WaitGroup{}
	mx := sync.RWMutex{}

	messages := []*sqs.Message{}
	for _, r := range requests {
		semaphore <- 1
		wg.Add(1)

		go func(r *sqs.ReceiveMessageInput) {
			result, err := c.client.ReceiveMessage(r)
			if err == nil && len(result.Messages) > 0 {
				mx.Lock()
				defer mx.Unlock()
				messages = append(messages, result.Messages...)
			} else if err != nil {
				logrus.Warn(err)
			}

			<-semaphore
			wg.Done()

		}(r)
	}
	wg.Wait()

	logrus.Debugf("consumer: pulled %d messages", len(messages))

	return messages
}

func (c *Consumer) dropMessages(messages []*sqs.Message) error {
	var err error

	semaphore := make(chan int, parallelRequests)
	defer close(semaphore)

	wg := &sync.WaitGroup{}

	for _, m := range messages {
		semaphore <- 1
		wg.Add(1)

		go func(m *sqs.Message) {
			err := c.dropMessage(m)
			if err != nil {
				logrus.Warn(err)
			} else {
				logrus.Debugf("consumer: deleted message from queue: %s", aws.StringValue(m.ReceiptHandle))
			}
			<-semaphore
			wg.Done()
		}(m)
	}
	wg.Wait()

	return err
}

func (c *Consumer) dropMessage(m *sqs.Message) error {
	params := &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(c.queueURL),
		ReceiptHandle: m.ReceiptHandle,
	}
	_, err := c.client.DeleteMessage(params)

	return err
}

func (c *Consumer) generateReceiveRequests() []*sqs.ReceiveMessageInput {
	ceil := float64(c.maxNumberOfMessages) / float64(maxMessagesPerRequest)
	numRequests := int(math.Ceil(ceil))

	var requests []*sqs.ReceiveMessageInput
	for i := 0; i < numRequests; i++ {
		requestNumMessages := c.maxNumberOfMessages % maxMessagesPerRequest
		if requestNumMessages == 0 {
			requestNumMessages = maxMessagesPerRequest
		}
		requests = append(requests, c.createReceiveRequest(requestNumMessages))
	}

	return requests
}

func (c *Consumer) createReceiveRequest(maxNumberOfMessages int64) *sqs.ReceiveMessageInput {
	input := &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(c.queueURL),
		MaxNumberOfMessages: aws.Int64(maxNumberOfMessages),
		AttributeNames: []*string{
			aws.String("All"),
		},
		WaitTimeSeconds: aws.Int64(c.waitTimeSeconds),
	}

	if c.visibilityTimeoutSeconds > 0 {
		input.VisibilityTimeout = aws.Int64(c.visibilityTimeoutSeconds)
	}

	return input
}
