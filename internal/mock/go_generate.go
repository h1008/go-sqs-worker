package mock

//go:generate mockgen -destination sqsMock.go -package $GOPACKAGE github.com/aws/aws-sdk-go/service/sqs/sqsiface SQSAPI
