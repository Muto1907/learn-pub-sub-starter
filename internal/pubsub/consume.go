package pubsub

import (
	"errors"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func DeclareAndBind(
	con *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType routing.SimpleQueueType,
) (*amqp.Channel, *amqp.Queue, error) {
	ch, err := con.Channel()
	if err != nil {
		return &amqp.Channel{}, &amqp.Queue{}, err
	}
	var durable, autoDelete, exclusive bool
	switch queueType {
	case routing.TRANSIENT:
		durable = false
		autoDelete = true
		exclusive = true
	case routing.DURABLE:
		durable = true
		autoDelete = false
		exclusive = false
	default:
		return &amqp.Channel{}, &amqp.Queue{}, errors.New("QueueType incompatible.")
	}
	queue, err := ch.QueueDeclare(
		queueName,
		durable,
		autoDelete,
		exclusive,
		false,
		nil,
	)
	if err != nil {
		return &amqp.Channel{}, &amqp.Queue{}, err
	}
	err = ch.QueueBind(queueName, key, exchange, false, nil)
	if err != nil {
		return &amqp.Channel{}, &amqp.Queue{}, err
	}
	return ch, &queue, nil
}
