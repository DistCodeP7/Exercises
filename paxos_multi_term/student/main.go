package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"strings"
	"time"

	"runner/shared"

	"github.com/distcodep7/dsnet/dsnet"
)

type PaxosNode struct {
	Net *dsnet.Node

	// Persistent state
	Term     int
	VotedFor string

	// Volatile state
	isLeader       bool
	votesReceived  map[string]map[string]bool
	lastElectionID string

	// Election management
	electionTimer  *time.Timer
	electionActive bool

	// Configuration
	peers      []string
	totalNodes int
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

	case "ElectionResult":
		var result shared.ElectionResult
		if err := json.Unmarshal(event.Payload, &result); err != nil {
			log.Fatalf("Failed to unmarshal ElectionResult: %v", err)
		}
		pn.handleElectionResult(ctx, result)
	}
}

func (pn *PaxosNode) startElection(ctx context.Context, electionID string) {
	// Become candidate
	pn.Term++
	pn.VotedFor = pn.Net.ID
	pn.isLeader = false
	pn.electionActive = true
	pn.lastElectionID = electionID

	// Initialize vote tracking with self-vote
	if pn.votesReceived == nil {
		pn.votesReceived = make(map[string]map[string]bool)
	}
	pn.votesReceived[electionID] = map[string]bool{pn.Net.ID: true}

	log.Printf("Node %s starting election (term: %d)", pn.Net.ID, pn.Term)

	for _, peerID := range pn.peers {
		if peerID != pn.Net.ID {
			req := shared.RequestVote{
				BaseMessage: dsnet.BaseMessage{From: pn.Net.ID, To: peerID, Type: "RequestVote"},
				Term:        pn.Term,
			}
			pn.Net.Send(ctx, peerID, req)
		}
	}

	if pn.electionTimer != nil {
		pn.electionTimer.Stop()
	}
	pn.electionTimer = time.AfterFunc(3*time.Second, func() {
		if pn.electionActive && !pn.isLeader {
			log.Printf("Node %s: Election timeout, retrying", pn.Net.ID)
			pn.startElection(ctx, electionID)
		}
	})
}

func (pn *PaxosNode) handleRequestVote(ctx context.Context, req shared.RequestVote) {
	if req.Term > pn.Term {
		pn.stepDown(req.Term)
	}

	// Grant vote if we haven't voted in this term or already voted for this candidate
	voteGranted := false
	if pn.VotedFor == "" || pn.VotedFor == req.From {
		pn.VotedFor = req.From
		voteGranted = true
		log.Printf("Node %s voted for %s in term %d", pn.Net.ID, req.From, pn.Term)

		if !pn.electionActive && pn.electionTimer != nil {
			pn.electionTimer.Stop()
		}
	}

	resp := shared.VoteResponse{
		BaseMessage: dsnet.BaseMessage{From: pn.Net.ID, To: req.From, Type: "VoteResponse"},
		Term:        req.Term,
		Granted:     voteGranted,
	}
	pn.Net.Send(ctx, req.From, resp)
}

func (pn *PaxosNode) stepDown(newTerm int) {
	pn.Term = newTerm
	pn.VotedFor = ""
	pn.isLeader = false
	pn.electionActive = false
	if pn.electionTimer != nil {
		pn.electionTimer.Stop()
	}
}

func (pn *PaxosNode) handleVoteResponse(ctx context.Context, resp shared.VoteResponse) {
	if resp.Term < pn.Term || !resp.Granted || pn.isLeader || !pn.electionActive {
		return
	}

	electionID := pn.lastElectionID
	if pn.votesReceived[electionID] == nil {
		pn.votesReceived[electionID] = make(map[string]bool)
	}

	pn.votesReceived[electionID][resp.From] = true
	votes := len(pn.votesReceived[electionID])
	majority := (pn.totalNodes / 2) + 1

	log.Printf("Node %s received vote from %s (%d/%d)", pn.Net.ID, resp.From, votes, majority)

	if votes >= majority {
		pn.becomeLeader(ctx, electionID)
	}
}

func (pn *PaxosNode) becomeLeader(ctx context.Context, electionID string) {
	pn.isLeader = true
	pn.electionActive = false
	if pn.electionTimer != nil {
		pn.electionTimer.Stop()
	}

	log.Printf("%s elected as leader (term: %d)", pn.Net.ID, pn.Term)

	for _, peerID := range pn.peers {
		electionResult := shared.ElectionResult{
			BaseMessage: dsnet.BaseMessage{
				From: "TESTER",
				To:   peerID,
				Type: "ElectionResult",
			},
			ElectionID: electionID,
			LeaderID:   pn.Net.ID,
			Success:    true,
		}
		pn.Net.Send(ctx, peerID, electionResult)
	}
	result := shared.ElectionResult{
		BaseMessage: dsnet.BaseMessage{From: pn.Net.ID, To: "TESTER", Type: "ElectionResult"},
		Success:     true,
		LeaderID:    pn.Net.ID,
		ElectionID:  electionID,
	}
	pn.Net.Send(ctx, "TESTER", result)
}

func (pn *PaxosNode) handleElectionResult(ctx context.Context, result shared.ElectionResult) {
	if pn.electionActive {
		log.Printf("Node %s: Election complete, %s is leader", pn.Net.ID, result.LeaderID)
		pn.electionActive = false
		if pn.electionTimer != nil {
			pn.electionTimer.Stop()
		}
	}
}
