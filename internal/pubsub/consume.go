package pubsub

import (
	"encoding/json"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

const (
	TRANSIENT SimpleQueueType = iota
	DURABLE
)

func DeclareAndBind(
	con *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {
	ch, err := con.Channel()
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("error creating channel: %v", err)
	}
	durable := queueType == DURABLE
	autoDelete := queueType == TRANSIENT
	exclusive := queueType == TRANSIENT
	queue, err := ch.QueueDeclare(
		queueName,
		durable,
		autoDelete,
		exclusive,
		false,
		nil,
	)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("error declaring queue %v", err)
	}
	err = ch.QueueBind(queueName, key, exchange, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("error binding queue: %v", err)
	}
	return ch, queue, nil
}

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T)) error {
	ch, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return fmt.Errorf("error declaring and binding queue: %v\n", err)
	}
	fmt.Printf("Queue %s declared and bound!", queue.Name)
	deliveryChan, err := ch.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("error consuming: %v\n", err)
	}
	go func() {
		for data := range deliveryChan {
			var payload T
			err := json.Unmarshal(data.Body, &payload)
			if err != nil {
				fmt.Printf("error unmarshalling delivery: %v\n", err)
				continue
			}
			handler(payload)
			err = data.Ack(false)
			if err != nil {
				log.Fatalf("error acknowleding delivery: %v\n", err)
				continue
			}
		}
	}()
	return nil
}
