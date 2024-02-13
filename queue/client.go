package queue

import (
	"context"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"time"
)

const waitOnError = 5 + time.Second

type optionFns []func(o *sqs.Options)

func NewSQSClient(config ClientConfig) (*sqs.Client, error) {
	cfg, err := awsConfig.LoadDefaultConfig(context.TODO())

	if err != nil {
		return nil, err
	}

	optFns := optionFns{}

	if config.Endpoint != nil {
		optFns = append(optFns, func(o *sqs.Options) {
			o.BaseEndpoint = config.Endpoint
			o.EndpointOptions.DisableHTTPS = true
		})
	}

	client := sqs.NewFromConfig(cfg, optFns...)

	return client, err
}
