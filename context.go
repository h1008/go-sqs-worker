package sqsworker

import (
	"context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type (
	Context interface {
		Context() context.Context
		WithContext(ctx context.Context) Context
		Message() *sqs.Message
		MessageBody() string
		Error(err error)
	}

	contextImpl struct {
		c            context.Context
		message      *sqs.Message
		errorHandler ErrorHandlerFunc
	}
)

func newContext(ctx context.Context, message *sqs.Message, handlerFunc ErrorHandlerFunc) Context {
	return &contextImpl{
		c:            ctx,
		message:      message,
		errorHandler: handlerFunc,
	}
}

func (c *contextImpl) Context() context.Context {
	return c.c
}

func (c *contextImpl) WithContext(ctx context.Context) Context {
	return &contextImpl{
		c:            ctx,
		message:      c.message,
		errorHandler: c.errorHandler,
	}
}

func (c *contextImpl) Message() *sqs.Message {
	return c.message
}

func (c *contextImpl) MessageBody() string {
	if c.message == nil {
		return ""
	}

	return aws.StringValue(c.message.Body)
}

func (c *contextImpl) Error(err error) {
	c.errorHandler(c, err)
}
