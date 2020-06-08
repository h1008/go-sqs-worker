package sqsworker_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestSQSWorker(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "SQS Worker Suite")
}
