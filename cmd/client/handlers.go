package main

import (
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

func HandlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.ACKTYPE {
	return func(ps routing.PlayingState) pubsub.ACKTYPE {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		return pubsub.ACK
	}
}

func HandlerMove(gs *gamelogic.GameState) func(gamelogic.ArmyMove) pubsub.ACKTYPE {
	return func(move gamelogic.ArmyMove) pubsub.ACKTYPE {
		defer fmt.Print("> ")
		outcome := gs.HandleMove(move)
		if outcome == gamelogic.MoveOutComeSafe || outcome == gamelogic.MoveOutcomeMakeWar {
			return pubsub.ACK
		}
		return pubsub.NACK_DISCARD
	}
}
