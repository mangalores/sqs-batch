package queue

import (
	"context"
	awsTypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/sirupsen/logrus"
	"sync"
)

type SingleHandler interface {
	Handle(ctx context.Context, msg awsTypes.Message) error
}

type BatchHandler interface {
	Handle(ctx context.Context, msg []awsTypes.Message) error
}

type WrapperHandler struct {
	handler SingleHandler
	wg      *sync.WaitGroup
}

func Wrap(handler SingleHandler) *WrapperHandler {
	return &WrapperHandler{
		handler: handler,
		wg:      &sync.WaitGroup{},
	}
}

func (h *WrapperHandler) Handle(ctx context.Context, messages []awsTypes.Message) error {
	for _, m := range messages {
		h.wg.Add(1)
		go h.consume(ctx, m)
	}

	h.wg.Wait()

	return nil
}

func (h *WrapperHandler) consume(ctx context.Context, m awsTypes.Message) {
	err := h.handler.Handle(ctx, m)
	if err != nil {
		logrus.Error(err)
	}

	h.wg.Done()
}
