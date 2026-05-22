package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
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

func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	err := encoder.Encode(val)
	if err != nil {
		return fmt.Errorf("error encoding gob: %v", err)
	}
	publishing := amqp.Publishing{
		ContentType: "application/gob",
		Body:        buf.Bytes(),
	}
	err = ch.PublishWithContext(
		context.Background(),
		exchange,
		key,
		false,
		false,
		publishing,
	)
	if err != nil {
		return fmt.Errorf("error publishing: %v", err)
	}
	return nil
}

func PublishGameLog(ch *amqp.Channel, username, message string) error {
	gl := routing.GameLog{
		CurrentTime: time.Now().UTC(),
		Message:     message,
		Username:    username,
	}
	err := PublishGob(ch, routing.ExchangePerilTopic, routing.GameLogSlug+"."+username, gl)
	if err != nil {
		return err
	}
	fmt.Printf("Publishing of Log successful: %v\n", gl)
	return nil
}
