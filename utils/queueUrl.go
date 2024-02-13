package utils

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/labstack/gommon/log"
)

type SQSQueueURLResolver interface {
	GetQueueUrl(ctx context.Context, input *sqs.GetQueueUrlInput, optFns ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error)
}

func GetQueueURL(client SQSQueueURLResolver, queueName string) (*string, error) {
	params := &sqs.GetQueueUrlInput{
		QueueName: aws.String(queueName), // Required
	}
	out, err := client.GetQueueUrl(context.TODO(), params)
	if err != nil {
		return nil, err
	}

	log.Debugf("sqs queue url is: %s", *out.QueueUrl)
	return out.QueueUrl, nil
}
