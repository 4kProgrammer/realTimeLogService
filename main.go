package main

import (
	"encoding/json"
	"log"
	"os"
	"strconv"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go"
	"github.com/joho/godotenv"
	"github.com/streadway/amqp"
)

type DeviceData struct {
	ID        int    `json:"id"`
	Data      string `json:"data"`
	Timestamp int64  `json:"timestamp"`
}

func main() {
	err := godotenv.Load(".env")
	if err != nil {
		log.Fatalf("failed to load environment variables: %v", err)
	}

	conn, err := amqp.Dial(os.Getenv("AMQP_URL"))
	if err != nil {
		log.Fatalf("failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("failed to open a channel: %v", err)
	}
	defer ch.Close()

	exchangeName := "data"
	exchangeType := "fanout"

	err = ch.ExchangeDeclare(
		exchangeName,
		exchangeType,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("failed to declare exchange: %v", err)
	}

	queueName := "influxdb_queue"

	_, err = ch.QueueDeclare(
		queueName,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("failed to declare queue: %v", err)
	}

	err = ch.QueueBind(
		queueName,
		"",
		exchangeName,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("failed to bind queue to exchange: %v", err)
	}

	influxDBClient := influxdb2.NewClientWithOptions(
		os.Getenv("INFLUXDB_URL"),
		os.Getenv("INFLUXDB_TOKEN"),
		influxdb2.DefaultOptions().SetBatchSize(20),
	)

	defer influxDBClient.Close()

	influxDBWriteAPI := influxDBClient.WriteAPI(
		os.Getenv("INFLUXDB_ORG"),
		os.Getenv("INFLUXDB_BUCKET"),
	)

	defer influxDBWriteAPI.Flush()

	ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	msgs, err := ch.Consume(
		queueName,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("failed to consume messages from queue: %v", err)
	}

	for msg := range msgs {
		var data DeviceData
		err := json.Unmarshal(msg.Body, &data)
		if err != nil {
			log.Printf("failed to unmarshal message body: %v", err)
			continue
		}

		p := influxdb2.NewPoint(
			"device_data",
			map[string]string{
				"id": strconv.Itoa(data.ID),
			},
			map[string]interface{}{
				"data":      data.Data,
				"timestamp": data.Timestamp,
			},
			time.Now(),
		)

		influxDBWriteAPI.WritePoint(p)
	}
}
