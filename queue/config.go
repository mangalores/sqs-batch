package queue

type ClientConfig struct {
	Endpoint     *string `envconfig:"AWS_SQS_URL" required:"false"`
	AWSAccessKey string  `envconfig:"AWS_ACCESS_KEY" required:"false"`
	AWSSecretKey string  `envconfig:"AWS_SECRET_KEY" required:"false"`
	AWSRegion    string  `envconfig:"AWS_REGION" default:"eu-central-1" required:"true"`
}

type ConsumerConfig struct {
	QueueName           string `envconfig:"AWS_SQS_QUEUE_NAME" required:"true"`
	MaxNumberOfMessages int32  `envconfig:"AWS_SQS_QUEUE_MAX_MESSAGES_PER_BATCH" default:"10"` // approximate, will round up to 10
	WaitTimeSeconds     int32  `envconfig:"AWS_SQS_QUEUE_WAIT_TIME" default:"5"`
	VisibilityTimeout   int32  `envconfig:"AWS_SQS_QUEUE_VISIBILITY_TIMEOUT" default:"60"`
}
