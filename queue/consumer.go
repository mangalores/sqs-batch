package queue

import (
	"context"
	"git.limango.tech/shop-catalog/libraries/sqs-queue.git/utils"
	"github.com/sirupsen/logrus"
	"math"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	awsTypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

const maxMessagesPerRequest = 10
const parallelRequests = 10

// SQSClient interface is the minimum interface required to consumeMessages a consumer.
// When a consumer is in its polling loop, it requires this interface.
type SQSClient interface {
	SQSReceiver
	utils.SQSQueueURLResolver
}
type SQSReceiver interface {
	DeleteMessageBatch(ctx context.Context, params *sqs.DeleteMessageBatchInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageBatchOutput, error)
	ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
}

// Consumer struct
type Consumer struct {
	queueURL            string
	maxNumberOfMessages int32
	waitTimeSeconds     int32
	visibilityTimeout   int32
	waitOnError         time.Duration

	handler BatchHandler
	client  SQSReceiver
}

func NewConsumer(config ConsumerConfig, client SQSClient, handler BatchHandler) (*Consumer, error) {
	queueUrl, err := utils.GetQueueURL(client, config.QueueName)
	if err != nil {
		return nil, err
	}

	return &Consumer{
		queueURL:            *queueUrl,
		maxNumberOfMessages: config.MaxNumberOfMessages,
		waitTimeSeconds:     config.WaitTimeSeconds,
		visibilityTimeout:   config.VisibilityTimeout,
		waitOnError:         waitOnError,

		handler: handler,
		client:  client,
	}, nil
}

// Start starts the polling and will continue polling till the application is forcibly stopped
func (c *Consumer) Start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			logrus.Debug("consumer: Stopping polling because a context kill signal was sent")
			return
		default:
			c.runBatch(ctx)
		}
	}
}

func (c *Consumer) runBatch(ctx context.Context) {
	messages := c.pullMessages(ctx)
	numMessages := len(messages)
	if numMessages > 0 {
		logrus.Infof("consumer: Received %d messages", numMessages)
		c.consumeMessages(ctx, messages)
		c.dropMessages(ctx, messages)
	}
}

func (c *Consumer) pullMessages(ctx context.Context) []awsTypes.Message {
	requests := c.generateReceiveRequests()

	semaphore := make(chan int, parallelRequests)
	defer close(semaphore)

	wg := &sync.WaitGroup{}
	mx := sync.RWMutex{}

	messages := []awsTypes.Message{}
	for _, r := range requests {
		semaphore <- 1
		wg.Add(1)

		go func(r *sqs.ReceiveMessageInput) {
			result, err := c.client.ReceiveMessage(ctx, r)
			if err == nil && len(result.Messages) > 0 {
				mx.Lock()
				defer mx.Unlock()
				messages = append(messages, result.Messages...)
			}

			<-semaphore
			wg.Done()

		}(r)
	}
	wg.Wait()

	logrus.Debugf("consumer: pulled %d messages", len(messages))

	return messages
}

// consumeMessages launches goroutine per received message and wait for all message to be processed
func (c *Consumer) consumeMessages(ctx context.Context, messages []awsTypes.Message) {
	if c.handler == nil {
		return
	}

	err := c.handler.Handle(ctx, messages)
	if err != nil {
		logrus.Error(err)
	}
}

func (c *Consumer) dropMessages(ctx context.Context, messages []awsTypes.Message) {
	semaphore := make(chan int, parallelRequests)
	defer close(semaphore)

	wg := &sync.WaitGroup{}

	bulk := []awsTypes.Message{}
	for i, m := range messages {

		bulk = append(bulk, m)

		if len(bulk) >= maxMessagesPerRequest || i == len(messages)-1 {
			semaphore <- 1
			wg.Add(1)

			go func(b []awsTypes.Message) {
				req := c.createBulkDeleteRequest(b)
				logDeleteResult(c.client.DeleteMessageBatch(ctx, req))

				<-semaphore
				wg.Done()
			}(bulk)

			bulk = []awsTypes.Message{}
		}
	}

	wg.Wait()
}

func logDeleteResult(result *sqs.DeleteMessageBatchOutput, err error) {
	if err != nil {
		logrus.Error(err)
		return
	}
	if result == nil {
		logrus.Error("sqs.DeleteMessageBatchOutput was empty")
		return
	}
	if len(result.Failed) > 0 {
		for _, fail := range result.Failed {
			logrus.Errorf("consumer: message deletion of %s failed with error '%s' (Code: %s)",
				aws.ToString(fail.Message), aws.ToString(fail.Message), aws.ToString(fail.Code))
		}
	}
	for _, success := range result.Successful {
		logrus.Debugf("consumer: deleted message %s from queue", aws.ToString(success.Id))
	}
}

func (c *Consumer) generateReceiveRequests() []*sqs.ReceiveMessageInput {
	ceil := float64(c.maxNumberOfMessages) / float64(maxMessagesPerRequest)
	numRequests := int(math.Ceil(ceil))

	var requests []*sqs.ReceiveMessageInput
	for i := 0; i < numRequests; i++ {
		requests = append(requests, c.createReceiveRequest(maxMessagesPerRequest))
	}

	return requests
}

func (c *Consumer) createReceiveRequest(maxMessagesPerRequest int32) *sqs.ReceiveMessageInput {

	input := &sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(c.queueURL),
		MaxNumberOfMessages:   maxMessagesPerRequest,
		VisibilityTimeout:     c.visibilityTimeout,
		MessageAttributeNames: []string{".*"},
		AttributeNames: []awsTypes.QueueAttributeName{
			"All",
		},
		WaitTimeSeconds: c.waitTimeSeconds,
	}

	return input
}

func (c *Consumer) createBulkDeleteRequest(messages []awsTypes.Message) *sqs.DeleteMessageBatchInput {

	entries := []awsTypes.DeleteMessageBatchRequestEntry{}
	for _, msg := range messages {
		entries = append(entries, awsTypes.DeleteMessageBatchRequestEntry{
			Id:            msg.MessageId,
			ReceiptHandle: msg.ReceiptHandle,
		})
	}

	return &sqs.DeleteMessageBatchInput{
		QueueUrl: aws.String(c.queueURL),
		Entries:  entries,
	}
}
