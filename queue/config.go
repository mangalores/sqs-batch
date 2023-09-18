package queue

type Config struct {
	Endpoint     *string `envconfig:"AWS_SQS_URL" required:"false"`
	AWSAccessKey string  `envconfig:"AWS_ACCESS_KEY" required:"false"`
	AWSSecretKey string  `envconfig:"AWS_SECRET_KEY" required:"false"`
	AWSRegion    string  `envconfig:"AWS_REGION" default:"eu-central-1" required:"true"`

	QueueName           string `envconfig:"AWS_SQS_QUEUE_NAME" required:"true"`
	MaxNumberOfMessages int64  `envconfig:"AWS_SQS_QUEUE_MAX_MESSAGES_PER_REQUEST" default:"10"`
	WaitTimeSeconds     int64  `envconfig:"AWS_SQS_QUEUE_WAIT_TIME" default:"5"`
	// how long should the messages be taken from queue aka expects worker to process them, "0" will not set any
	VisibilityTimeoutSeconds int64 `envconfig:"AWS_SQS_QUEUE_BLOCK_TIME" default:"0"`
}
