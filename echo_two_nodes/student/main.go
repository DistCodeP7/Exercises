package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"strings"

	"runner/shared"

	"github.com/distcodep7/dsnet/dsnet"
)

var Peers []string

type EchoNode struct{ Net *dsnet.Node }

func NewEchoNode(id string) *EchoNode {
	n, err := dsnet.NewNode(id, "test-container:50051")
	if err != nil {
		log.Fatalf("Failed to create node %s: %v", id, err)
	}

	return &EchoNode{Net: n}
}

func newBaseMessage(from, to, msgType string) dsnet.BaseMessage {
	return dsnet.BaseMessage{From: from, To: to, Type: msgType}
}

func main() {
	id := os.Getenv("ID")
	if id == "" {
		log.Fatal("ID environment variable not set")
		return
	}
	Peers = strings.Split(os.Getenv("PEERS"), ",")
	if Peers == nil {
		log.Fatal("PEERS environment variable not set")
		return
	}

	ctx := context.Background()
	echoNode := NewEchoNode(id)
	defer echoNode.Net.Close()
	go echoNode.Run(ctx)
	select {}
}

func (en *EchoNode) Run(ctx context.Context) {
	defer en.Net.Close()
	for {
		select {
		case event := <-en.Net.Inbound:
			en.handleEvent(ctx, event)
		case <-ctx.Done():
			os.Exit(0)
		}
	}
}

func (en *EchoNode) handleEvent(ctx context.Context, event dsnet.Event) {
	switch event.Type {
	case "SendTrigger":
		var msg shared.SendTrigger
		json.Unmarshal(event.Payload, &msg)
		en.Net.Send(ctx, Peers[1], shared.EchoMessage{
			BaseMessage: newBaseMessage(en.Net.ID, Peers[1], "EchoMessage"),
			EchoID:      msg.EchoID,
			Content:     msg.Content,
		})
		log.Printf("Sending message to %s", Peers[1])
	case "EchoMessage":
		var msg shared.EchoMessage
		json.Unmarshal(event.Payload, &msg)

		en.Net.Send(ctx, msg.From, shared.EchoResponse{
			BaseMessage: newBaseMessage(en.Net.ID, msg.From, "EchoResponse"),
			EchoID:      msg.EchoID,
			Content:     msg.Content,
		})
		log.Printf("Echoing message back to %s", msg.From)
	case "EchoResponse":
		var resp shared.EchoResponse
		json.Unmarshal(event.Payload, &resp)

		en.Net.Send(ctx, "TESTER", shared.ReplyReceived{
			BaseMessage: newBaseMessage(en.Net.ID, "TESTER", "ReplyReceived"),
			EchoID:      resp.EchoID,
			Success:     true,
		})
	}
}
