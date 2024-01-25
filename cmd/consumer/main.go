package main

import (
	"log"

	"kafka-go-poc/internal/kafka"
	"kafka-go-poc/pkg/ping"
)

const (
	topic             = "ping.v1"
	kafkaURL          = "localhost:19092"
	schemaRegistryURL = "http://localhost:8081"
)

func main() {
	consumer, err := kafka.NewConsumer(kafkaURL, schemaRegistryURL)
	defer consumer.Close()

	if err != nil {
		log.Fatal(err)
	}
	messageType := (&ping.PingMessage{}).ProtoReflect().Type()
	if err := consumer.Run(messageType, topic); err != nil {
		log.Fatal(err)
	}
}
