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

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("error creating channel: %v", err)
	}

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("error parsing username: %v", err)
	}
	queueName := fmt.Sprintf("%s.%s", routing.PauseKey, username)
	gameState := gamelogic.NewGameState(username)
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilDirect,
		queueName,
		routing.PauseKey,
		pubsub.TRANSIENT,
		HandlerPause(gameState))
	if err != nil {
		log.Fatalf("error subscribing to %s", queueName)
	}

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		routing.ArmyMovesPrefix+"."+username,
		routing.ArmyMovesPrefix+".*",
		pubsub.TRANSIENT,
		HandlerMove(gameState, ch),
	)
	if err != nil {
		log.Fatalf("error subscribing to %s.*", routing.ArmyMovesPrefix)
	}
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		"war",
		routing.WarRecognitionsPrefix+".*",
		pubsub.DURABLE,
		HandlerWar(gameState))

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
			err = pubsub.PublishJson(
				ch,
				routing.ExchangePerilTopic,
				routing.ArmyMovesPrefix+"."+username,
				move,
			)
			if err != nil {
				fmt.Printf("error publishing move: %v", err)
			}
			fmt.Printf("move to %s successfully published\n", move.ToLocation)
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
