package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")
	URL := "amqp://guest:guest@localhost:5672"
	conn, err := amqp.Dial(URL)
	if err != nil {
		log.Fatalf("error connecting to rabbitmq: %v", err)
	}
	fmt.Println("Connection to rabbitmq successful!")
	defer conn.Close()
	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("%v", err)
	}
	queueName := fmt.Sprintf("%s.%s", routing.PauseKey, username)
	ch, queue, err := pubsub.DeclareAndBind(
		conn,
		routing.ExchangePerilDirect,
		queueName,
		routing.PauseKey,
		routing.TRANSIENT)

	if err != nil {
		log.Fatalf("%v", err)
	}
	defer ch.Close()
	fmt.Printf("Created and Bound to queue %s", queue.Name)

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt)
	<-signalCh
	fmt.Println("Connection closed")
}
