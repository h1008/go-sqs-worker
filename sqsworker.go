package sqsworker

import (
	"context"
	"sync"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
)

type MiddlewareFunc func(next HandlerFunc) HandlerFunc
type HandlerFunc func(c Context) error
type ErrorHandlerFunc func(c Context, err error)

type Consumer struct {
	ErrorHandler ErrorHandlerFunc
	client       sqsiface.SQSAPI
	middleware   []MiddlewareFunc
	handlers     []*handler
	waitGroup    sync.WaitGroup
}

func NewConsumer() *Consumer {
	svc := sqs.New(session.Must(session.NewSession()))
	return NewConsumerWithClient(svc)
}

func NewConsumerWithClient(client sqsiface.SQSAPI) *Consumer {
	return &Consumer{
		client:    client,
		waitGroup: sync.WaitGroup{},
	}
}

func (c *Consumer) Use(middleware ...MiddlewareFunc) {
	c.middleware = append(c.middleware, middleware...)
}

func (c *Consumer) Handle(queueURL string, handle HandlerFunc) {
	if c.ErrorHandler == nil {
		c.ErrorHandler = func(_ Context, _ error) {}
	}

	c.handlers = append(c.handlers, &handler{
		consumer: c,
		queueURL: queueURL,
		handle:   handle,
	})
}

func (c *Consumer) Run() {
	for _, h := range c.handlers {
		h.run(context.Background())
	}
}

func (c *Consumer) Shutdown(ctx context.Context) error {
	c.cancelReceives()

	done := make(chan struct{})
	go func() {
		c.waitGroup.Wait()
		done <- struct{}{}
	}()

	select {
	case <-ctx.Done():
		c.cancelHandlers()
		return ctx.Err()
	case <-done:
		return nil
	}
}

func (c *Consumer) cancelHandlers() {
	for _, h := range c.handlers {
		if h.cancelHandler != nil {
			h.cancelHandler()
		}
	}
}

func (c *Consumer) cancelReceives() {
	for _, h := range c.handlers {
		if h.cancelReceive != nil {
			h.cancelReceive()
		}
	}
}
