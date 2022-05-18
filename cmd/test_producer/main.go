package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/Shopify/sarama"
	"github.com/riferrei/srclient"
	"github.com/urfave/cli/v2"
)

var schemaString = `{
  "$schema": "draft_7",
  "properties": {
    "time": {
      "type": "string",
      "format": "date-time"
    }
  },
  "required": [
    "time"
  ],
  "type": "object"
}`

func main() {
	app := &cli.App{
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "broker",
				Value:   "localhost:9092",
				EnvVars: []string{"BROKER"},
			},
			&cli.StringFlag{
				Name:    "sr",
				Value:   "http://localhost:8081",
				EnvVars: []string{"SCHEMA_REGISTRY"},
			},
		},
		Action: run,
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

func run(c *cli.Context) error {
	saramaCfg := sarama.NewConfig()
	saramaCfg.Producer.Return.Successes = true
	broker := c.String("broker")

	// Make topic
	clusterAdmin, err := sarama.NewClusterAdmin([]string{broker}, saramaCfg)
	if err != nil {
		return fmt.Errorf("can not make cluster admin: %w", err)
	}

	topics, err := clusterAdmin.ListTopics()
	if err != nil {
		return fmt.Errorf("can not list topics: %w", err)
	}
	if _, ok := topics["test"]; !ok {
		if err := clusterAdmin.CreateTopic("test", &sarama.TopicDetail{
			NumPartitions:     1,
			ReplicationFactor: 1,
		}, false); err != nil {
			return fmt.Errorf("can not make topic: %w", err)
		}
	}

	// Make schema
	src := srclient.CreateSchemaRegistryClient(c.String("sr"))
	schema, err := src.CreateSchema("topic-value", schemaString, srclient.Json)
	if err != nil {
		return fmt.Errorf("can not register schema: %w", err)
	}

	fmt.Println("SCHEMA ID", schema.ID())

	// Produce
	producer, err := sarama.NewSyncProducer([]string{broker}, saramaCfg)
	if err != nil {
		return fmt.Errorf("can not connect to kfk")
	}

	msg := struct {
		Time string `json:"time"`
	}{
		Time: time.Now().String(),
	}
	value, _ := json.Marshal(msg)

	schemaIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(schemaIDBytes, uint32(schema.ID()))

	if _, _, err := producer.SendMessage(&sarama.ProducerMessage{
		Topic:     "test",
		Value:     sarama.ByteEncoder(append(schemaIDBytes, value...)),
		Timestamp: time.Now(),
	}); err != nil {
		return fmt.Errorf("can not produce: %w", err)
	}
	return nil
}
