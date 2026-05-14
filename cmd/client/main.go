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
		pubsub.TRANSIENT)

	if err != nil {
		log.Fatalf("%v", err)
	}
	defer ch.Close()
	fmt.Printf("Created and Bound to queue %s\n", queue.Name)

	gameState := gamelogic.NewGameState(username)
	for {
		cmd := gamelogic.GetInput()
		if len(cmd) == 0 {
			continue
		}
		switch cmd[0] {
		case "spawn":
			err = gameState.CommandSpawn(cmd)
			if err != nil {
				fmt.Printf("error spawning unit: %v\n", err)
				continue
			}
		case "move":
			move, err := gameState.CommandMove(cmd)
			if err != nil {
				fmt.Printf("error moving unit: %v\n", err)
				continue
			}
			fmt.Printf("move to %s successful", move.ToLocation)
		case "status":
			gameState.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Println("Spamming not allowed yet!")
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Println("error: Command doesn't exist")
			continue
		}
	}
}
