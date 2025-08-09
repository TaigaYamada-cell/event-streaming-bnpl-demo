package producer

import (
	"context"
	"encoding/json"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
)

const streamName = "atone-events-stream"

// EmitEventはKinesisストリームにイベントを発行する
func EmitEvent(ctx context.Context, client *kinesis.Client, event interface{}) error {
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}

	_, err = client.PutRecord(ctx, &kinesis.PutRecordInput{
		Data:         data,
		StreamName:   aws.String(streamName),
		PartitionKey: aws.String("partition-key-1"),
	})

	if err != nil {
		return err
	}

	log.Printf("Event emitted to Kinesis: %s\n", data)
	return nil
}