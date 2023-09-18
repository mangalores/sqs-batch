package queue

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/labstack/gommon/log"
	"go.elastic.co/apm/module/apmawssdkgo"
	"time"
)

const waitOnError = 5 + time.Second

// SQSClient interface is the minimum interface required to consume a consumer.
// When a consumer is in its polling loop, it requires this interface.
type SQSClient interface {
	SQSReceiver
	GetQueueUrl(input *sqs.GetQueueUrlInput) (*sqs.GetQueueUrlOutput, error)
}

func NewSQSConsumer(config Config, client SQSClient) (*Consumer, error) {
	queueUrl, err := GetQueueURL(client, config.QueueName)
	if err != nil {
		return nil, err
	}

	return &Consumer{
		queueURL:                 *queueUrl,
		maxNumberOfMessages:      config.MaxNumberOfMessages,
		waitTimeSeconds:          config.WaitTimeSeconds,
		visibilityTimeoutSeconds: config.VisibilityTimeoutSeconds,
		client:                   client,
		waitOnError:              waitOnError,
	}, nil
}

func NewSQSClient(config Config) *sqs.SQS {
	awsConfig := &aws.Config{
		Credentials: getCredentials(config.AWSAccessKey, config.AWSSecretKey),
		Region:      aws.String(config.AWSRegion),
		Endpoint:    config.Endpoint,
	}

	awsSession := session.Must(session.NewSession(awsConfig))
	awsSession = apmawssdkgo.WrapSession(awsSession)

	return sqs.New(awsSession, awsConfig)
}

func GetQueueURL(client SQSClient, queueName string) (*string, error) {
	params := &sqs.GetQueueUrlInput{
		QueueName: aws.String(queueName), // Required
	}
	out, err := client.GetQueueUrl(params)
	if err != nil {
		return out.QueueUrl, err
	}

	log.Debugf("sqs queue url is: %s", *out.QueueUrl)
	return out.QueueUrl, nil
}

func getCredentials(awsAccessKey string, awsSecretKey string) *credentials.Credentials {
	if awsAccessKey == "" || awsSecretKey == "" {
		return nil
	}
	return credentials.NewStaticCredentials(awsAccessKey, awsSecretKey, "")
}
