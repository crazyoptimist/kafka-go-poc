package main

import (
	"fmt"
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
	producer, err := kafka.NewProducer(kafkaURL, schemaRegistryURL)
	defer producer.Close()

	if err != nil {
		log.Fatal(err)
	}
	pingMSG := ping.PingMessage{Value: 55}
	offset, err := producer.ProduceMessage(&pingMSG, topic)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Produced a message successfully. Offset: %v\n", offset)
}
