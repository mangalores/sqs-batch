package queue

import (
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNewSQSClientOK(t *testing.T) {
	config := ClientConfig{}
	client, err := NewSQSClient(config)

	require.NotNil(t, client)
	assert.Nil(t, err)
	assert.IsType(t, &sqs.Client{}, client)
}
