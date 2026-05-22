package main

import (
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func HandlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.ACKTYPE {
	return func(ps routing.PlayingState) pubsub.ACKTYPE {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		return pubsub.ACK
	}
}

func HandlerMove(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.ArmyMove) pubsub.ACKTYPE {
	return func(move gamelogic.ArmyMove) pubsub.ACKTYPE {
		defer fmt.Print("> ")
		outcome := gs.HandleMove(move)
		switch outcome {
		case gamelogic.MoveOutComeSafe:
			return pubsub.ACK

		case gamelogic.MoveOutcomeMakeWar:
			routingKey := fmt.Sprintf("%s.%s", routing.WarRecognitionsPrefix, gs.GetUsername())
			payload := gamelogic.RecognitionOfWar{
				Attacker: move.Player,
				Defender: gs.GetPlayerSnap(),
			}
			err := pubsub.PublishJson(ch, routing.ExchangePerilTopic, routingKey, payload)
			if err != nil {
				fmt.Printf("error publishing warRecognition: %v\n", err)
				return pubsub.NACK_REQUEUE
			}
			return pubsub.ACK

		case gamelogic.MoveOutcomeSamePlayer:
			return pubsub.NACK_DISCARD

		default:
			fmt.Printf("error: move: %v not recognized\n", move)
			return pubsub.NACK_DISCARD
		}
	}
}

func HandlerWar(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.RecognitionOfWar) pubsub.ACKTYPE {
	return func(rw gamelogic.RecognitionOfWar) pubsub.ACKTYPE {
		defer fmt.Printf("> ")
		outcome, winner, loser := gs.HandleWar(rw)
		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NACK_REQUEUE
		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NACK_DISCARD
		case gamelogic.WarOutcomeDraw:
			err := pubsub.PublishGameLog(
				ch,
				rw.Attacker.Username,
				fmt.Sprintf("A war between %s and %s resulted in a draw", winner, loser),
			)
			if err != nil {
				fmt.Printf("error publishing log: %v", err)
				return pubsub.NACK_REQUEUE
			}
			return pubsub.ACK
		case gamelogic.WarOutcomeYouWon:
			fallthrough
		case gamelogic.WarOutcomeOpponentWon:
			err := pubsub.PublishGameLog(
				ch,
				rw.Attacker.Username,
				fmt.Sprintf("%s won a war against %s", winner, loser),
			)
			if err != nil {
				fmt.Printf("error publishing log: %v", err)
				return pubsub.NACK_REQUEUE
			}
			return pubsub.ACK
		default:
			fmt.Printf("error outcome %v not recognized\n", outcome)
			return pubsub.NACK_DISCARD
		}
	}
}
