package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

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
	ch, err := connection.Channel()
	if err != nil {
		log.Fatalf("error creating channel: %v", err)
	}
	err = pubsub.PublishJson(ch, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true})
	if err != nil {
		log.Fatalf("error publishing: %v", err)
	}
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Println("Shutting down...")
}
