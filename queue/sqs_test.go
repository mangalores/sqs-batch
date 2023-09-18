package queue

import (
	"errors"
	awsCredentials "github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNewSQSConsumerOK(t *testing.T) {
	client := &MockClient{
		queueUrl: "https://foo.bar/baz",
	}

	config := Config{}
	consumer, err := NewSQSConsumer(config, client)

	require.Nil(t, err)

	assert.IsType(t, &Consumer{}, consumer)
}

func TestNewSQSConsumerGetQueueUrlErr(t *testing.T) {
	expectedErr := errors.New("foo bar baz")
	client := &MockClient{
		queueUrl:    "https://foo.bar/baz",
		queueUrlErr: expectedErr,
	}

	config := Config{}
	consumer, err := NewSQSConsumer(config, client)

	require.NotNil(t, err)
	assert.Nil(t, consumer)
	assert.Equal(t, expectedErr, err)
}

func TestNewSQSClientOK(t *testing.T) {
	config := Config{}
	client := NewSQSClient(config)

	require.NotNil(t, client)
	assert.IsType(t, &sqs.SQS{}, client)
}

func TestGetStaticCredentials(t *testing.T) {
	credentials1 := getCredentials("foo", "bar")
	require.NotNil(t, credentials1)
	assert.IsType(t, &awsCredentials.Credentials{}, credentials1)

	credentials2 := getCredentials("", "")
	assert.Nil(t, credentials2)
	assert.IsType(t, &awsCredentials.Credentials{}, credentials2)
}
