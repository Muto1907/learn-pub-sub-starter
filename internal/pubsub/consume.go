package pubsub

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

const (
	TRANSIENT SimpleQueueType = iota
	DURABLE
)

type ACKTYPE int

const (
	ACK ACKTYPE = iota
	NACK_REQUEUE
	NACK_DISCARD
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
		amqp.Table{
			"x-dead-letter-exchange": "peril_dlx",
		},
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
	handler func(T) ACKTYPE,
) error {
	unmarshaller := func(data []byte) (T, error) {
		var val T
		err := json.Unmarshal(data, &val)
		if err != nil {
			return val, fmt.Errorf("error unmarshalling json: %v", err)
		}
		return val, nil
	}
	return Subscribe(
		con,
		exchange,
		queueName,
		key,
		queueType,
		handler,
		unmarshaller,
	)
}

func SubscribeGob[T any](
	con *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) ACKTYPE,
) error {
	unmarshaller := func(data []byte) (T, error) {
		buf := bytes.NewBuffer(data)
		decoder := gob.NewDecoder(buf)
		var val T
		err := decoder.Decode(&val)
		if err != nil {
			return val, fmt.Errorf("error decoding gob: %v\n", err)
		}
		return val, nil
	}
	return Subscribe(
		con,
		exchange,
		queueName,
		key,
		queueType,
		handler,
		unmarshaller,
	)
}

func Subscribe[T any](
	con *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) ACKTYPE,
	unmarshaller func(data []byte) (T, error),
) error {
	ch, queue, err := DeclareAndBind(con, exchange, queueName, key, queueType)
	if err != nil {
		return fmt.Errorf("error declaring and binding to queue: %v\n", err)
	}
	fmt.Printf("successfully bound to queue: %s\n", queue.Name)
	deliveryChan, err := ch.Consume(
		queueName,
		"",
		false,
		false,
		false,
		false,
		nil)
	if err != nil {
		return fmt.Errorf("error consuming queue: %v", err)
	}
	go func() {
		defer ch.Close()
		for data := range deliveryChan {
			val, err := unmarshaller(data.Body)
			if err != nil {
				fmt.Printf("error unmarshalling from queue: %v\n", err)
			}
			switch handler(val) {
			case ACK:
				data.Ack(false)
				fmt.Printf("ACK sent for msg: %v\n", val)
			case NACK_REQUEUE:
				data.Nack(false, true)
				fmt.Printf("Nack with requeue sent for msg: %v\n", val)
			case NACK_DISCARD:
				data.Nack(false, false)
				fmt.Printf("Nack Discard sent for msg: %v\n", val)
			}
		}
	}()
	return nil
}
