package pubsub

import (
	"encoding/json"
	"fmt"

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
	con *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T),
) error {
	ch, queue, err := DeclareAndBind(
		con,
		exchange,
		queueName,
		key,
		queueType,
	)
	if err != nil {
		return fmt.Errorf("error declaring and binding queue: %v", err)
	}
	defer ch.Close()
	fmt.Printf("successfully bound to queue %s\n", queue.Name)

	deliveryChan, err := ch.Consume(
		queueName,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	unmarshaller := func(data []byte) (T, error) {
		var target T
		err = json.Unmarshal(data, &target)
		if err != nil {
			return target, err
		}
		return target, nil
	}
	go func() {
		for data := range deliveryChan {
			msg, err := unmarshaller(data.Body)
			if err != nil {
				fmt.Printf("error unmarshalling delivery: %v", err)
			}
			handler(msg)
		}
	}()
	return nil
}
