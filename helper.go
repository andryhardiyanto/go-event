package event

import (
	"encoding/json"
	"fmt"

	"github.com/IBM/sarama"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

func Unmarshal(msg any, req any) error {
	var dataBytes []byte
	switch typedMsg := msg.(type) {
	case *sarama.ConsumerMessage: // kafka
		dataBytes = typedMsg.Value
	case types.Message: // sqs
		if typedMsg.Body != nil {
			dataBytes = []byte(*typedMsg.Body)
		}
	default:
		return fmt.Errorf("msg is not *sarama.ConsumerMessage (kafka) or types.Message (sqs)")
	}

	err := json.Unmarshal(dataBytes, req)
	if err != nil {
		return err
	}

	return nil
}
