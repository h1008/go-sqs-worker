package sqsworker_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	sqs2 "github.com/aws/aws-sdk-go/service/sqs"
	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	sqsworker "go-sqs-worker"
	"go-sqs-worker/internal/mock"
)

const queueURL = "someQueueURL"

var _ = Describe("SQS Consumer", func() {
	var ctrl *gomock.Controller
	var sqsAPIMock *mock.MockSQSAPI
	var consumer *sqsworker.Consumer

	givenMessagesInQueue := func(messages ...*sqs2.ReceiveMessageOutput) {
		for _, message := range messages {
			sqsAPIMock.EXPECT().
				ReceiveMessageWithContext(gomock.Any(), getReceiveParams(queueURL)).
				Return(message, nil)
		}
		sqsAPIMock.EXPECT().
			ReceiveMessageWithContext(gomock.Any(), gomock.Any()).
			Return(&sqs2.ReceiveMessageOutput{}, nil).
			AnyTimes()
	}

	expectMessageToBeDeleted := func(message *sqs2.Message) {
		expectedInput := &sqs2.DeleteMessageInput{
			QueueUrl:      aws.String(queueURL),
			ReceiptHandle: message.ReceiptHandle,
		}

		sqsAPIMock.EXPECT().
			DeleteMessageWithContext(gomock.Any(), expectedInput).
			Return(&sqs2.DeleteMessageOutput{}, nil)
	}

	ignoreFurtherMessageDeletions := func() {
		sqsAPIMock.EXPECT().
			DeleteMessageWithContext(gomock.Any(), gomock.Any()).
			Return(&sqs2.DeleteMessageOutput{}, nil).
			AnyTimes()
	}

	BeforeEach(func() {
		ctrl = gomock.NewController(GinkgoT())
		sqsAPIMock = mock.NewMockSQSAPI(ctrl)

		consumer = sqsworker.NewConsumerWithClient(sqsAPIMock)
	})

	AfterEach(func() {
		ctrl.Finish()
	})

	Describe("Run", func() {
		It("should do nothing if no handler is registered", func() {
			consumer.Run()
		})

		It("should call handler for each received message", func() {
			body1 := "body1"
			body2 := "body2"

			message := givenMessageWithBodies(body1, body2)

			givenMessagesInQueue(message)
			ignoreFurtherMessageDeletions()

			receivedBody := make(chan string, 2)
			consumer.Handle(queueURL, func(c sqsworker.Context) error {
				receivedBody <- c.MessageBody()
				return nil
			})

			consumer.Run()

			Eventually(receivedBody).Should(Receive(Equal(body1)))
			Eventually(receivedBody).Should(Receive(Equal(body2)))
		})

		It("should call handler for all messages in multiple receive calls", func() {
			body1 := "body1"
			body2 := "body2"

			message1 := givenMessageWithBodies(body1)
			message2 := givenMessageWithBodies(body2)

			givenMessagesInQueue(message1, message2)
			ignoreFurtherMessageDeletions()

			receivedBody := make(chan string, 2)
			consumer.Handle(queueURL, func(c sqsworker.Context) error {
				receivedBody <- c.MessageBody()
				return nil
			})

			consumer.Run()

			Eventually(receivedBody).Should(Receive(Equal(body1)))
			Eventually(receivedBody).Should(Receive(Equal(body2)))
		})

		It("should process multiple handlers for different queues", func() {
			anotherQueueURL := "someOtherQueueURL"
			body1 := "body1"
			body2 := "body2"
			body3 := "body3"
			body4 := "body4"

			messageQueue1 := givenMessageWithBodies(body1, body2)
			sqsAPIMock.EXPECT().
				ReceiveMessageWithContext(gomock.Any(), getReceiveParams(queueURL)).
				Return(messageQueue1, nil)

			messageQueue2 := givenMessageWithBodies(body3, body4)
			sqsAPIMock.EXPECT().
				ReceiveMessageWithContext(gomock.Any(), getReceiveParams(anotherQueueURL)).
				Return(messageQueue2, nil)

			sqsAPIMock.EXPECT().
				ReceiveMessageWithContext(gomock.Any(), gomock.Any()).
				Return(&sqs2.ReceiveMessageOutput{}, nil).
				AnyTimes()

			ignoreFurtherMessageDeletions()

			receivedBodyQueue1 := make(chan string, 2)
			consumer.Handle(queueURL, func(c sqsworker.Context) error {
				receivedBodyQueue1 <- c.MessageBody()
				return nil
			})

			receivedBodyQueue2 := make(chan string, 2)
			consumer.Handle(anotherQueueURL, func(c sqsworker.Context) error {
				receivedBodyQueue2 <- c.MessageBody()
				return nil
			})

			consumer.Run()

			Eventually(receivedBodyQueue1).Should(Receive(Equal(body1)))
			Eventually(receivedBodyQueue1).Should(Receive(Equal(body2)))

			Eventually(receivedBodyQueue2).Should(Receive(Equal(body3)))
			Eventually(receivedBodyQueue2).Should(Receive(Equal(body4)))
		})

		It("should delete all successfully handled messages", func(done Done) {
			lastMessage := "body4"
			message1 := givenMessageWithBodies("body1", "body2")
			message2 := givenMessageWithBodies("body3", lastMessage)

			givenMessagesInQueue(message1, message2)
			expectMessageToBeDeleted(message1.Messages[0])
			expectMessageToBeDeleted(message1.Messages[1])
			expectMessageToBeDeleted(message2.Messages[0])
			expectMessageToBeDeleted(message2.Messages[1])

			consumer.Handle(queueURL, func(c sqsworker.Context) error {
				if c.MessageBody() == lastMessage {
					close(done)
				}
				return nil
			})

			consumer.Run()
		})

		When("processing a message fails", func() {
			body1 := "body1"
			body2 := "body2"

			It("should call the error handler", func() {
				message := givenMessageWithBodies(body1)
				givenMessagesInQueue(message)
				ignoreFurtherMessageDeletions()

				someError := errors.New("some error")
				consumer.Handle(queueURL, func(c sqsworker.Context) error {
					return someError
				})

				receivedErrors := make(chan error, 1)
				consumer.ErrorHandler = func(c sqsworker.Context, err error) {
					receivedErrors <- err
				}

				consumer.Run()

				Eventually(receivedErrors).Should(Receive(Equal(someError)))
			})

			It("should not delete the failed message", func(done Done) {
				message := givenMessageWithBodies(body1)
				givenMessagesInQueue(message)

				sqsAPIMock.EXPECT().
					DeleteMessageWithContext(gomock.Any(), gomock.Any()).
					Return(&sqs2.DeleteMessageOutput{}, nil).
					Times(0)

				consumer.Handle(queueURL, func(c sqsworker.Context) error {
					close(done)
					return errors.New("an error")
				})

				consumer.Run()
			})

			It("should continue to receive messages", func(done Done) {
				message1 := givenMessageWithBodies(body1)
				message2 := givenMessageWithBodies(body2)

				givenMessagesInQueue(message1, message2)
				ignoreFurtherMessageDeletions()

				consumer.Handle(queueURL, func(c sqsworker.Context) error {
					if c.MessageBody() == body1 {
						return errors.New("an error")
					}
					close(done)
					return nil
				})

				consumer.Run()
			})
		})

		When("receiving a message fails", func() {
			It("should call the error handler", func() {
				someError := errors.New("some error")
				sqsAPIMock.EXPECT().
					ReceiveMessageWithContext(gomock.Any(), getReceiveParams(queueURL)).
					Return(nil, someError).
					AnyTimes()

				consumer.Handle(queueURL, func(c sqsworker.Context) error {
					return nil
				})

				receivedErrors := make(chan error, 1)
				consumer.ErrorHandler = func(c sqsworker.Context, err error) {
					receivedErrors <- err
				}

				consumer.Run()

				Eventually(receivedErrors).Should(Receive(Equal(someError)))
			})

			It("should continue to receive messages", func(done Done) {
				message := givenMessageWithBodies("body1")

				sqsAPIMock.EXPECT().
					ReceiveMessageWithContext(gomock.Any(), gomock.Any()).
					Return(nil, errors.New("an error"))
				givenMessagesInQueue(message)

				ignoreFurtherMessageDeletions()

				consumer.Handle(queueURL, func(c sqsworker.Context) error {
					close(done)
					return nil
				})

				consumer.Run()
			})
		})

		When("deleting a message fails", func() {
			It("should call the error handler", func() {
				message := givenMessageWithBodies("body1")
				givenMessagesInQueue(message)

				someError := errors.New("some error")
				sqsAPIMock.EXPECT().
					DeleteMessageWithContext(gomock.Any(), gomock.Any()).
					Return(nil, someError).
					AnyTimes()

				consumer.Handle(queueURL, func(c sqsworker.Context) error {
					return nil
				})

				receivedErrors := make(chan error, 1)
				consumer.ErrorHandler = func(c sqsworker.Context, err error) {
					receivedErrors <- err
				}

				consumer.Run()

				Eventually(receivedErrors).Should(Receive(Equal(someError)))
			})

			It("should continue to receive messages", func(done Done) {
				message1 := givenMessageWithBodies("body1")
				message2 := givenMessageWithBodies("body2")
				givenMessagesInQueue(message1, message2)

				sqsAPIMock.EXPECT().
					DeleteMessageWithContext(gomock.Any(), gomock.Any()).
					Return(nil, errors.New("an error"))
				ignoreFurtherMessageDeletions()

				consumer.Handle(queueURL, func(c sqsworker.Context) error {
					if c.MessageBody() == "body2" {
						close(done)
					}
					return nil
				})

				consumer.Run()
			})
		})

		Context("Middleware", func() {
			BeforeEach(func() {
				message := givenMessageWithBodies("body1")

				givenMessagesInQueue(message)
				ignoreFurtherMessageDeletions()
			})

			It("should call middleware functions in the correct order", func() {
				ch := make(chan string)
				middleware := func(i int) sqsworker.MiddlewareFunc {
					return func(next sqsworker.HandlerFunc) sqsworker.HandlerFunc {
						return func(c sqsworker.Context) error {
							ch <- fmt.Sprintf("%d-before", i)
							err := next(c)
							ch <- fmt.Sprintf("%d-after", i)
							return err
						}
					}
				}

				consumer.Use(middleware(1), middleware(2))
				consumer.Use(middleware(3))

				consumer.Handle(queueURL, func(c sqsworker.Context) error {
					ch <- "handler"
					return nil
				})

				consumer.Run()

				Eventually(ch).Should(Receive(Equal("1-before")))
				Eventually(ch).Should(Receive(Equal("2-before")))
				Eventually(ch).Should(Receive(Equal("3-before")))
				Eventually(ch).Should(Receive(Equal("handler")))
				Eventually(ch).Should(Receive(Equal("3-after")))
				Eventually(ch).Should(Receive(Equal("2-after")))
				Eventually(ch).Should(Receive(Equal("1-after")))
			})

			It("should be able to call the error handler", func() {
				consumer.Use(func(next sqsworker.HandlerFunc) sqsworker.HandlerFunc {
					return func(c sqsworker.Context) error {
						c.Error(next(c))
						return nil
					}
				})

				someError := errors.New("someError")
				consumer.Handle(queueURL, func(c sqsworker.Context) error {
					return someError
				})

				receivedErrors := make(chan error, 1)
				consumer.ErrorHandler = func(c sqsworker.Context, err error) {
					receivedErrors <- err
				}

				consumer.Run()

				Eventually(receivedErrors).Should(Receive(Equal(someError)))
			})

			It("should be able to replace the context", func(done Done) {
				contextKey := "key"
				contextValue := "value"
				consumer.Use(func(next sqsworker.HandlerFunc) sqsworker.HandlerFunc {
					return func(c sqsworker.Context) error {
						ctx := context.WithValue(c.Context(), contextKey, contextValue)
						return next(c.WithContext(ctx))
					}
				})

				consumer.Handle(queueURL, func(c sqsworker.Context) error {
					defer GinkgoRecover()
					Expect(c.Context().Value(contextKey)).To(Equal(contextValue))
					close(done)
					return nil
				})

				consumer.Run()
			})
		})
	})

	Describe("Shutdown", func() {
		It("should do nothing if no handler is registered", func() {
			Expect(consumer.Shutdown(context.Background())).To(Succeed())
		})

		It("should stop waiting receive calls immediately", func() {
			sqsAPIMock.EXPECT().
				ReceiveMessageWithContext(gomock.Any(), getReceiveParams(queueURL)).
				DoAndReturn(func(ctx context.Context, in *sqs2.ReceiveMessageInput) (*sqs2.ReceiveMessageOutput, error) {
					select {
					case <-ctx.Done():
						return nil, awserr.New("an error", "an error", ctx.Err())
					}
				})

			consumer.Handle(queueURL, func(c sqsworker.Context) error {
				return nil
			})

			consumer.Run()

			Expect(consumer.Shutdown(context.Background())).To(Succeed())
		})

		It("should finish processing the current messages", func() {
			message := givenMessageWithBodies("body1")

			givenMessagesInQueue(message)

			canceled := make(chan bool)
			consumer.Handle(queueURL, func(c sqsworker.Context) error {
				select {
				case <-c.Context().Done():
					canceled <- true
				}
				return nil
			})

			consumer.Run()

			go func() {
				defer GinkgoRecover()
				Expect(consumer.Shutdown(context.Background())).To(Succeed())
			}()

			Consistently(canceled, time.Second).ShouldNot(Receive())
		})

		It("should cancel the handler context and stop processing when the provided context is canceled", func() {
			message := givenMessageWithBodies("body1")

			givenMessagesInQueue(message)
			ignoreFurtherMessageDeletions()

			canceled := make(chan bool)
			consumer.Handle(queueURL, func(c sqsworker.Context) error {
				select {
				case <-c.Context().Done():
					canceled <- true
				}
				return nil
			})

			consumer.Run()

			ctx, cancel := context.WithCancel(context.Background())
			go func() {
				defer GinkgoRecover()
				Expect(consumer.Shutdown(ctx)).NotTo(Succeed())
			}()

			cancel()

			Eventually(canceled).Should(Receive(Equal(true)))
		})
	})
})

func givenMessageWithBodies(messageBodies ...string) *sqs2.ReceiveMessageOutput {
	r := new(sqs2.ReceiveMessageOutput)
	for idx, body := range messageBodies {
		r.Messages = append(r.Messages, &sqs2.Message{
			Body:          aws.String(body),
			ReceiptHandle: aws.String(fmt.Sprintf("rh-%d", idx)),
		})
	}
	return r
}

func getReceiveParams(queueURL string) *sqs2.ReceiveMessageInput {
	return &sqs2.ReceiveMessageInput{
		AttributeNames: []*string{
			aws.String(sqs2.QueueAttributeNameAll),
		},
		MessageAttributeNames: []*string{
			aws.String(sqs2.QueueAttributeNameAll),
		},
		QueueUrl:            aws.String(queueURL),
		MaxNumberOfMessages: aws.Int64(1),
		VisibilityTimeout:   aws.Int64(20),
		WaitTimeSeconds:     aws.Int64(0),
	}
}
