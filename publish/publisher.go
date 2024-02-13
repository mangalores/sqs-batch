package publish

import (
	"context"
	"fmt"
	"git.limango.tech/shop-catalog/libraries/sqs-queue.git/utils"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/gofrs/uuid"
	"github.com/labstack/gommon/log"
	"strings"
)

type SQSPublisher interface {
	SendMessage(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
	SendMessageBatch(ctx context.Context, params *sqs.SendMessageBatchInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error)
	utils.SQSQueueURLResolver
}

type MessageParser interface {
	Parse(message interface{}) (MessageParams, error)
}

type Publisher struct {
	QueueURL string
	IsFIFO   bool
	Parser   MessageParser
	client   SQSPublisher
}

func NewPublisher(config PublisherConfig, client SQSPublisher) (*Publisher, error) {
	queueURL, err := utils.GetQueueURL(client, config.QueueName)
	if err != nil {
		return nil, err
	}

	return &Publisher{
		QueueURL: *queueURL,
		IsFIFO:   config.IsFIFO,
		Parser:   NewDefaultMessageParser(),
		client:   client,
	}, nil
}

func (p *Publisher) Publish(ctx context.Context, message interface{}) (err error) {
	input, err := p.createSendMessageInput(message)
	if err != nil {
		return
	}

	output, err := p.client.SendMessage(ctx, input)
	if err == nil {
		log.Debugf("published message %s", output.MessageId)
	}

	return
}

func (p *Publisher) PublishBatch(ctx context.Context, messages []interface{}) error {
	entries, err := p.createSendMessageEntries(messages)
	if err != nil {
		return err
	}

	input := &sqs.SendMessageBatchInput{
		Entries:  entries,
		QueueUrl: aws.String(p.QueueURL),
	}

	output, err := p.client.SendMessageBatch(ctx, input)
	if err == nil {
		for _, o := range output.Successful {
			log.Debugf("published message %s", o.MessageId)
		}

		if len(output.Failed) > 0 {
			return NewPartialError(output)
		}
	}

	return err
}

func (p *Publisher) createSendMessageInput(message interface{}) (*sqs.SendMessageInput, error) {
	params, err := p.Parser.Parse(message)
	if err != nil {
		return nil, err
	}

	return &sqs.SendMessageInput{
		QueueUrl:               aws.String(p.QueueURL),
		MessageBody:            aws.String(params.Body),
		MessageGroupId:         p.fifoOnly(params.MessageGroupID),
		MessageDeduplicationId: p.fifoOnly(params.DeduplicationID),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"Message-Type": {DataType: aws.String("String"), StringValue: aws.String(params.MessageType)},
			"Content-Type": {DataType: aws.String("String"), StringValue: aws.String(params.ContentType)},
		},
	}, nil
}

func (p *Publisher) createSendMessageEntries(messages []interface{}) ([]types.SendMessageBatchRequestEntry, error) {
	entries := []types.SendMessageBatchRequestEntry{}

	for _, m := range messages {
		entry, err := p.createSendMessageEntry(m)
		if err != nil {
			return nil, err
		}

		entries = append(entries, *entry)
	}

	return entries, nil
}

func (p *Publisher) createSendMessageEntry(message interface{}) (*types.SendMessageBatchRequestEntry, error) {
	params, err := p.Parser.Parse(message)
	if err != nil {
		return nil, err
	}

	// just need unique id for request
	key, _ := uuid.NewV4()
	return &types.SendMessageBatchRequestEntry{
		Id:                     aws.String(key.String()),
		MessageBody:            aws.String(params.Body),
		MessageGroupId:         p.fifoOnly(params.MessageGroupID),
		MessageDeduplicationId: p.fifoOnly(params.DeduplicationID),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"Message-Type": {DataType: aws.String("String"), StringValue: aws.String(params.MessageType)},
			"Content-Type": {DataType: aws.String("String"), StringValue: aws.String(params.ContentType)},
		},
	}, nil
}

func (p *Publisher) fifoOnly(value string) *string {
	if p.IsFIFO {
		return aws.String(value)
	} else {
		return nil
	}
}

type PartialError struct {
	Total  int
	Errors []types.BatchResultErrorEntry
}

func NewPartialError(output *sqs.SendMessageBatchOutput) PartialError {
	return PartialError{
		Total:  len(output.Failed) + len(output.Successful),
		Errors: output.Failed,
	}
}

func (e PartialError) Error() string {
	return fmt.Sprintf("%d of %d messages failed to publish:\n%s", len(e.Errors), e.Total, e.stringifyErrors())
}

func (e PartialError) stringifyErrors() string {
	errors := []string{}

	for _, err := range e.Errors {
		errors = append(errors, fmt.Sprintf("%s [Code:%s,EntryId:%s]", *err.Message, *err.Code, *err.Id))
	}

	return strings.Join(errors, ", ")
}
