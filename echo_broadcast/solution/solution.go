package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"distcode/echo"

	"github.com/distcodep7/dsnet/dsnet"
)

var totalNodes int

type EchoNode struct {
	Net            *dsnet.Node
	pendingReplies map[string]map[string]bool // echoID -> map[nodeID]bool
}

func NewEchoNode(id string) *EchoNode {
	n, err := dsnet.NewNode(id, "test:50051")
	if err != nil {
		fmt.Println("Failed to create node %s: %v", id, err)
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
		var msg echo.SendTrigger
		json.Unmarshal(event.Payload, &msg)

		if en.pendingReplies == nil {
			en.pendingReplies = make(map[string]map[string]bool)
		}
		en.pendingReplies[msg.EchoID] = make(map[string]bool)

		en.SendToAll(ctx, msg.EchoID, msg.Content)
	case "EchoMessage":
		var msg echo.EchoMessage
		json.Unmarshal(event.Payload, &msg)

		en.Net.Send(ctx, msg.From, echo.EchoResponse{
			BaseMessage: newBaseMessage(en.Net.ID, msg.From, "EchoResponse"),
			EchoID:      msg.EchoID,
			Content:     msg.Content,
		})
	case "EchoResponse":
		// Handle EchoResponse
		var resp echo.EchoResponse
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
			en.Net.Send(ctx, "TESTER", echo.ReplyReceived{
				BaseMessage: newBaseMessage(en.Net.ID, "TESTER", "ReplyReceived"),
				EchoID:      resp.EchoID,
				Success:     true,
			})
			os.Exit(0)
		}
	}
}

func (en *EchoNode) SendToAll(ctx context.Context, echoID string, content string) {
	for i := 1; i <= totalNodes; i++ {
		nodeID := fmt.Sprintf("replica%d", i)
		if nodeID == en.Net.ID {
			continue // skip self
		}

		en.Net.Send(ctx, nodeID, echo.EchoMessage{
			BaseMessage: newBaseMessage(en.Net.ID, nodeID, "EchoMessage"),
			EchoID:      echoID,
			Content:     content,
		})
	}
}

func main() {
	time.Sleep(3 * time.Second)

	id := os.Getenv("NODE_ID")
	if id == "" {
		fmt.Println("NODE_ID environment variable not set")
		return
	}

	peers := os.Getenv("PEERS")
	if peers == "" {
		fmt.Println("PEERS environment variable not set")
		return
	}

	var peersList []string
	if err := json.Unmarshal([]byte(peers), &peersList); err != nil {
		fmt.Println("Error parsing peers JSON:", err)
		return
	}
	totalNodes = len(peersList)

	EchoNode := NewEchoNode(id)
	if EchoNode == nil {
		fmt.Println("Failed to create echo node")
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	EchoNode.Run(ctx)
}
