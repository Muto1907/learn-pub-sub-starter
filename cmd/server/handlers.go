package main

import (
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

func HandlerLog() func(routing.GameLog) pubsub.ACKTYPE {
	return func(gl routing.GameLog) pubsub.ACKTYPE {
		defer fmt.Print("> ")
		err := gamelogic.WriteLog(gl)
		if err != nil {
			fmt.Printf("error writing log: %v\n", err)
			return pubsub.NACK_REQUEUE
		}
		return pubsub.ACK
	}
}
