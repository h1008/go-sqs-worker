package sqsworker

import (
	"context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type handler struct {
	consumer      *Consumer
	queueURL      string
	handle        HandlerFunc
	cancelReceive context.CancelFunc
	cancelHandler context.CancelFunc
}

func (h *handler) run(ctx context.Context) {
	h.consumer.waitGroup.Add(1)

	handler := applyMiddleware(h.consumer.middleware, h.handleAndDelete)

	receiveCtx, cancelReceive := context.WithCancel(ctx)
	h.cancelReceive = cancelReceive

	handleCtx, cancelHandler := context.WithCancel(ctx)
	h.cancelHandler = cancelHandler

	go func() {
		defer h.consumer.waitGroup.Done()

		for {
			result, err := h.consumer.client.ReceiveMessageWithContext(receiveCtx, &sqs.ReceiveMessageInput{
				AttributeNames: []*string{
					aws.String(sqs.QueueAttributeNameAll),
				},
				MessageAttributeNames: []*string{
					aws.String(sqs.QueueAttributeNameAll),
				},
				QueueUrl:            aws.String(h.queueURL),
				MaxNumberOfMessages: aws.Int64(1),
				VisibilityTimeout:   aws.Int64(20),
				WaitTimeSeconds:     aws.Int64(0),
			})

			if err != nil {
				if err, ok := err.(awserr.Error); ok && err.OrigErr() == context.Canceled {
					return
				}
				h.consumer.ErrorHandler(newContext(receiveCtx, nil, h.consumer.ErrorHandler), err)
				continue
			}

			for _, message := range result.Messages {
				c := newContext(handleCtx, message, h.consumer.ErrorHandler)
				err := handler(c)
				if err != nil {
					h.consumer.ErrorHandler(c, err)
				}
			}
		}
	}()
}

func (h *handler) handleAndDelete(context Context) error {
	if err := h.handle(context); err != nil {
		return err
	}

	_, err := h.consumer.client.DeleteMessageWithContext(context.Context(), &sqs.DeleteMessageInput{
		QueueUrl:      &h.queueURL,
		ReceiptHandle: context.Message().ReceiptHandle,
	})
	return err
}

func applyMiddleware(middleware []MiddlewareFunc, handler HandlerFunc) HandlerFunc {
	for i := len(middleware) - 1; i >= 0; i-- {
		handler = middleware[i](handler)
	}
	return handler
}
