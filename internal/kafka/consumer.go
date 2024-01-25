package kafka

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry/serde/protobuf"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const (
	consumerGroupID       = "ping-consumer"
	defaultSessionTimeout = 6000
	noTimeout             = -1
)

type Consumer interface {
	Run(messageType protoreflect.MessageType, topic string) error
	Close()
}

type kafkaConsumer struct {
	consumer     *kafka.Consumer
	deserializer *protobuf.Deserializer
}

var _ Consumer = (*kafkaConsumer)(nil)

func NewConsumer(kafkaURL, srURL string) (Consumer, error) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  kafkaURL,
		"group.id":           consumerGroupID,
		"session.timeout.ms": defaultSessionTimeout,
		"enable.auto.commit": false,
	})
	if err != nil {
		return nil, err
	}

	sr, err := schemaregistry.NewClient(schemaregistry.NewConfig(srURL))
	if err != nil {
		return nil, err
	}

	d, err := protobuf.NewDeserializer(sr, serde.ValueSerde, protobuf.NewDeserializerConfig())
	if err != nil {
		return nil, err
	}
	return &kafkaConsumer{
		consumer:     c,
		deserializer: d,
	}, nil
}

func (c *kafkaConsumer) RegisterMessage(messageType protoreflect.MessageType) error {
	return nil
}

func (c *kafkaConsumer) Run(messageType protoreflect.MessageType, topic string) error {
	if err := c.consumer.SubscribeTopics([]string{topic}, nil); err != nil {
		return err
	}
	if err := c.deserializer.ProtoRegistry.RegisterMessage(messageType); err != nil {
		return err
	}
	for {
		kafkaMsg, err := c.consumer.ReadMessage(noTimeout)
		if err != nil {
			return err
		}
		msg, err := c.deserializer.Deserialize(topic, kafkaMsg.Value)
		if err != nil {
			return err
		}
		c.handleMessage(msg, int64(kafkaMsg.TopicPartition.Offset))
		if _, err = c.consumer.CommitMessage(kafkaMsg); err != nil {
			return err
		}
	}
}

func (c *kafkaConsumer) handleMessage(message interface{}, offset int64) {
	fmt.Printf("Handled message %v with offset %d\n", message, offset)
}

func (c *kafkaConsumer) Close() {
	if err := c.consumer.Close(); err != nil {
		log.Fatal(err)
	}
	c.deserializer.Close()
}
