package queue

import (
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/sirupsen/logrus"
	"sync"
)

type SingleHandler interface {
	Handle(msg *sqs.Message) error
}

type WrapperHandler struct {
	handler SingleHandler
}

func Wrap(handler SingleHandler) *WrapperHandler {
	return &WrapperHandler{
		handler: handler,
	}
}

func (h *WrapperHandler) Handle(messages []*sqs.Message) error {
	wg := &sync.WaitGroup{}

	for _, m := range messages {
		wg.Add(1)
		go h.consume(m, wg)
	}

	wg.Wait()

	return nil
}

func (h *WrapperHandler) consume(m *sqs.Message, wg *sync.WaitGroup) {
	err := h.handler.Handle(m)
	if err != nil {
		logrus.Error(err)
	}

	wg.Done()
}
