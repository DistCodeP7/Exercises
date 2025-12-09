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

type PaxosNode struct {
	Net   *dsnet.Node
	State struct {
		Term     int
		VotedFor string
	}

	votesReceived  map[string]map[string]bool
	lastElectionID string
	isLeader       bool
	totalNodes     int
	peers          []string
}

func NewPaxosNode(id string, peers []string) *PaxosNode {
	node, err := dsnet.NewNode(id, "test-container:50051")
	if err != nil {
		log.Fatalf("Failed to create node %s: %v", id, err)
	}
	return &PaxosNode{
		Net:        node,
		totalNodes: len(peers),
		peers:      peers,
	}
}

func (pn *PaxosNode) Run(ctx context.Context) {
	for {
		select {
		case event := <-pn.Net.Inbound:
			pn.handleEvent(ctx, event)
		case <-ctx.Done():
			return
		}
	}
}

func (pn *PaxosNode) handleEvent(ctx context.Context, event dsnet.Event) {
	switch event.Type {
	case "ElectionTrigger":
		var msg shared.ElectionTrigger
		if err := json.Unmarshal(event.Payload, &msg); err != nil {
			log.Fatalf("Failed to unmarshal ElectionTrigger: %v", err)
		}
		pn.startElection(ctx, msg.ElectionID)

	case "RequestVote":
		var req shared.RequestVote
		if err := json.Unmarshal(event.Payload, &req); err != nil {
			log.Fatalf("Failed to unmarshal RequestVote: %v", err)
		}
		pn.handleRequestVote(ctx, req)

	case "VoteResponse":
		var resp shared.VoteResponse
		if err := json.Unmarshal(event.Payload, &resp); err != nil {
			log.Fatalf("Failed to unmarshal VoteResponse: %v", err)
		}
		pn.handleVoteResponse(ctx, resp)
	}
}

func (pn *PaxosNode) handleRequestVote(ctx context.Context, req shared.RequestVote) {
	voteGranted := false

	// Higher term → update and reset vote
	if req.Term > pn.State.Term {
		pn.State.Term = req.Term
		pn.State.VotedFor = ""
	}

	// Grant vote if not voted yet or voting for same candidate
	if pn.State.VotedFor == "" {
		pn.State.VotedFor = req.From
		voteGranted = true
		log.Printf("Node %s voted for %s in term %d", pn.Net.ID, req.From, req.Term)
	} else if pn.State.VotedFor == req.From {
		voteGranted = true
		log.Printf("Node %s voted again for %s in term %d", pn.Net.ID, req.From, req.Term)
	}

	resp := shared.VoteResponse{
		BaseMessage: dsnet.BaseMessage{From: pn.Net.ID, To: req.From, Type: "VoteResponse"},
		Term:        req.Term,
		Granted:     voteGranted,
	}
	pn.Net.Send(ctx, req.From, resp)
}

func (pn *PaxosNode) handleVoteResponse(ctx context.Context, resp shared.VoteResponse) {
	electionID := pn.lastElectionID
	if pn.votesReceived[electionID] == nil {
		pn.votesReceived[electionID] = make(map[string]bool)
	}

	if resp.Granted {
		pn.votesReceived[electionID][resp.From] = true
	}

	votes := len(pn.votesReceived[electionID])
	neededVotes := (pn.totalNodes / 2) + 1

	if votes >= neededVotes && !pn.isLeader {
		pn.isLeader = true

		log.Printf("%s elected as leader with a majority of votes", pn.Net.ID)
		result := shared.ElectionResult{
			BaseMessage: dsnet.BaseMessage{From: pn.Net.ID, To: "TESTER", Type: "ElectionResult"},
			Success:     true,
			LeaderID:    pn.Net.ID,
			ElectionID:  "TEST_001",
		}
		pn.Net.Send(ctx, "TESTER", result)
	}
}

func (pn *PaxosNode) startElection(ctx context.Context, electionID string) {
	pn.State.Term++
	pn.State.VotedFor = pn.Net.ID // vote for self

	pn.lastElectionID = electionID

	if pn.votesReceived == nil {
		pn.votesReceived = make(map[string]map[string]bool)
	}
	pn.votesReceived[electionID] = map[string]bool{
		pn.Net.ID: true, // count own vote
	}

	pn.sendToAll(ctx)
}

func (pn *PaxosNode) sendToAll(ctx context.Context) {
	for _, peerID := range pn.peers {
		if peerID != pn.Net.ID {
			req := shared.RequestVote{
				BaseMessage: dsnet.BaseMessage{From: pn.Net.ID, To: peerID, Type: "RequestVote"},
				Term:        pn.State.Term,
			}
			pn.Net.Send(ctx, peerID, req)
		}
	}
}

func main() {
	id := os.Getenv("ID")
	if id == "" {
		log.Fatal("ID environment variable not set")
	}

	peers := strings.Split(os.Getenv("PEERS"), ",")
	if len(peers) == 0 {
		log.Fatal("PEERS environment variable not set")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node := NewPaxosNode(id, peers)
	node.Run(ctx)
}
