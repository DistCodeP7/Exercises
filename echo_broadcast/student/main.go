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

var totalNodes 	int
var Peers 		[]string
var id          string

type EchoNode struct {
	Net            *dsnet.Node
	pendingReplies map[string]map[string]bool // echoID -> map[nodeID]bool
}

func NewEchoNode(id string) *EchoNode {
	n, err := dsnet.NewNode(id, "test-container:50051")
	if err != nil {
		log.Fatalf("Failed to create node %s: %v\n", id, err)
		os.Exit(1)
	}
	return &EchoNode{Net: n, pendingReplies: make(map[string]map[string]bool)}
}

func newBaseMessage(from, to, msgType string) dsnet.BaseMessage {
	return dsnet.BaseMessage{
		From: from,
		To:   to,
		Type: msgType,
	}
}

func (en *EchoNode) Run(ctx context.Context) {
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

		if en.pendingReplies == nil {
			en.pendingReplies = make(map[string]map[string]bool)
		}
		en.pendingReplies[msg.EchoID] = make(map[string]bool)
        log.Printf("ASS2")
		en.SendToAll(ctx, msg.EchoID, msg.Content)
	case "EchoMessage":
		var msg shared.EchoMessage
		json.Unmarshal(event.Payload, &msg)

		en.Net.Send(ctx, msg.From, shared.EchoResponse{
			BaseMessage: newBaseMessage(en.Net.ID, msg.From, "EchoResponse"),
			EchoID:      msg.EchoID,
			Content:     msg.Content,
		})
	case "EchoResponse":
		// Handle EchoResponse
		var resp shared.EchoResponse
		json.Unmarshal(event.Payload, &resp)

		if en.pendingReplies == nil {
			en.pendingReplies = make(map[string]map[string]bool)
		}
		if en.pendingReplies[resp.EchoID] == nil {
			en.pendingReplies[resp.EchoID] = make(map[string]bool)
		}

		fromNode := resp.From
		if fromNode == en.Net.ID {
			return // ignore self
		}

		// only mark first response from a node
		if !en.pendingReplies[resp.EchoID][fromNode] {
			en.pendingReplies[resp.EchoID][fromNode] = true
		}

		if len(en.pendingReplies[resp.EchoID]) == totalNodes-1 {
			// All replies received
			en.Net.Send(ctx, "TESTER", 	shared.ReplyReceived{
				BaseMessage: newBaseMessage(en.Net.ID, "TESTER", "ReplyReceived"),
				EchoID:      resp.EchoID,
				Success:     true,
			})
		}
	}
}

func (en *EchoNode) SendToAll(ctx context.Context, echoID string, content string) {
	for i := 1; i <= totalNodes; i++ {
		nodeID := Peers[i-1]
		if nodeID == en.Net.ID {
			continue // skip self
		}

		en.Net.Send(ctx, nodeID, shared.EchoMessage{
			BaseMessage: newBaseMessage(en.Net.ID, nodeID, "EchoMessage"),
			EchoID:      echoID,
			Content:     content,
		})
	}
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
	totalNodes = len(Peers)

	ctx := context.Background()
	echoNode := NewEchoNode(id)
	defer echoNode.Net.Close()
	echoNode.Run(ctx)
}
