package publish

type PublisherConfig struct {
	QueueName string `envconfig:"AWS_SQS_QUEUE_NAME" required:"true"`
	IsFIFO    bool   `envconfig:"AWS_SQS_FIFO_QUEUE"`
}
