package pubsub

import (
	"context"
	"encoding/json"

	amqp "github.com/rabbitmq/amqp091-go"
)

func PublishJson[T any](ch *amqp.Channel, exchange, key string, val T) error {
	jsondata, err := json.Marshal(val)
	if err != nil {
		return err
	}
	publishing := amqp.Publishing{
		ContentType: "application/json",
		Body:        jsondata,
	}

	err = ch.PublishWithContext(context.Background(), exchange, key, false, false, publishing)
	if err != nil {
		return err
	}
	return nil
}
