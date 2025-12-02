package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"time"

	"distcode/echo"

	"github.com/distcodep7/dsnet/dsnet"
)

type EchoNode struct{ Net *dsnet.Node }

func NewEchoNode(id string) *EchoNode {
	n, err := dsnet.NewNode(id, "test:50051")
	if err != nil {
		log.Fatalf("Failed to create node %s: %v", id, err)
	}
	return &EchoNode{Net: n}
}

func newBaseMessage(from, to, msgType string) dsnet.BaseMessage {
	return dsnet.BaseMessage{From: from, To: to, Type: msgType}
}

func (en *EchoNode) Run(ctx context.Context) {
	defer en.Net.Close()
	for {
		select {
		case event := <-en.Net.Inbound:
			en.handleEvent(ctx, event)
		case <-ctx.Done():
			return
		}
	}
}

func (en *EchoNode) handleEvent(ctx context.Context, event dsnet.Event) {
	switch event.Type {
	case "SendTrigger":
		var msg echo.SendTrigger
		json.Unmarshal(event.Payload, &msg)
		en.Net.Send(ctx, "replica2", echo.EchoMessage{
			BaseMessage: newBaseMessage(en.Net.ID, "replica2", "EchoMessage"),
			EchoID:      msg.EchoID,
			Content:     msg.Content,
		})
	case "EchoMessage":
		var msg echo.EchoMessage
		json.Unmarshal(event.Payload, &msg)

		en.Net.Send(ctx, msg.From, echo.EchoResponse{
			BaseMessage: newBaseMessage(en.Net.ID, msg.From, "EchoResponse"),
			EchoID:      msg.EchoID,
			Content:     msg.Content,
		})
	case "EchoResponse":
		var resp echo.EchoResponse
		json.Unmarshal(event.Payload, &resp)

		en.Net.Send(ctx, "TESTER", echo.ReplyReceived{
			BaseMessage: newBaseMessage(en.Net.ID, "TESTER", "ReplyReceived"),
			EchoID:      resp.EchoID,
			Success:     true,
		})
		return
	}
}

func main() {
	time.Sleep(3 * time.Second)

	id := os.Getenv("NODE_ID")
	if id == "" {
		log.Printf("NODE_ID environment variable not set")
		return
	}
	peer := os.Getenv("PEER")
	if peer == "" {
		log.Printf("PEER environment variable not set")
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	echoNode := NewEchoNode(id)
	echoNode.Run(ctx) // Block here instead of goroutine
}
