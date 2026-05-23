package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")
	connectionURL := "amqp://guest:guest@localhost:5672/"
	connection, err := amqp.Dial(connectionURL)
	if err != nil {
		log.Fatalf("error creating amqp connection: %v", err)
		return
	}
	defer connection.Close()
	fmt.Println("Connection successful")
	gamelogic.PrintServerHelp()
	ch, err := connection.Channel()
	if err != nil {
		log.Fatalf("error creating channel: %v", err)
	}
	err = pubsub.SubscribeGob(
		connection,
		routing.ExchangePerilTopic,
		routing.GameLogSlug,
		routing.GameLogSlug+".*",
		pubsub.DURABLE,
		HandlerLog(),
	)
	if err != nil {
		log.Fatalf("error, declaring and binding queue")
	}
	defer ch.Close()
	for {
		command := gamelogic.GetInput()
		if len(command) == 0 {
			continue
		}
		switch command[0] {
		case "pause":
			fmt.Println("Sending Pause message")
			pubsub.PublishJson(
				ch,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{IsPaused: true},
			)

		case "resume":
			fmt.Println("Sending resume message")
			pubsub.PublishJson(
				ch,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{IsPaused: false},
			)

		case "quit":
			fmt.Printf("Exiting...\n")
			return

		default:
			fmt.Printf("Command not found\n")
		}
	}
}
