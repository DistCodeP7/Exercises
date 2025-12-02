package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	ex "distcode/mutex"

	"github.com/distcodep7/dsnet/dsnet"
)

type MutexNode struct {
	Net           *dsnet.Node
	CoordinatorID string
}

// Centralized coordinator state
type state struct {
	// client-side
	waitingGrant  bool
	mutexID       string
	workMillis    int
	coordinatorID string

	// coordinator-side
	isCoordinator bool
	started       bool
	inCS          bool
	holder        string
	queue         []string
	completed     map[string]bool
	allNodes      []string
}

func NewMutexNode(id string, coordinatorID string) *MutexNode {
	n, err := dsnet.NewNode(id, "test:50051")
	if err != nil {
		log.Fatalf("Failed to create node %s: %v", id, err)
	}
	return &MutexNode{Net: n, CoordinatorID: coordinatorID}
}

func (en *MutexNode) Run(ctx context.Context) {
	defer en.Net.Close()
	// Detect if coordinator based on whether CoordinatorID is empty
	isCoordinator := en.CoordinatorID == ""
	coordinatorID := en.CoordinatorID
	if isCoordinator {
		coordinatorID = en.Net.ID // Coordinator references itself
	}

	st := state{
		isCoordinator: isCoordinator,
		started:       false,
		inCS:          false,
		holder:        "",
		queue:         []string{},
		workMillis:    300,
		completed:     map[string]bool{},
		allNodes:      []string{},
		coordinatorID: coordinatorID,
	}

	for {
		select {
		case event := <-en.Net.Inbound:
			handleEvent(ctx, en, &st, event)
		case <-ctx.Done():
			return
		}
	}
}

func handleEvent(ctx context.Context, en *MutexNode, st *state, event dsnet.Event) {
	switch event.Type {
	case "MutexTrigger":
		var trig ex.MutexTrigger
		if err := json.Unmarshal(event.Payload, &trig); err != nil {
			return
		}
		st.mutexID = trig.MutexID
		if trig.WorkMillis > 0 {
			st.workMillis = trig.WorkMillis
		}

		if st.isCoordinator {
			if st.started {
				return
			}
			st.started = true
			// Coordinator reads peers list from env
			peersJSON := os.Getenv("peers_json")
			if err := json.Unmarshal([]byte(peersJSON), &st.allNodes); err != nil {
				log.Printf("Coordinator failed to parse peers_json: %v", err)
				return
			}
			// Ask all other nodes to request once
			for _, id := range st.allNodes {
				if id == en.Net.ID {
					continue
				}
				t := ex.MutexTrigger{BaseMessage: dsnet.BaseMessage{From: en.Net.ID, To: id, Type: "MutexTrigger"}, MutexID: st.mutexID, WorkMillis: st.workMillis}
				en.Net.Send(ctx, id, t)
			}
			// Coordinator takes its own turn first
			st.inCS = true
			doCoordinatorCS(ctx, en, st)
		} else {
			// Send request to coordinator and wait for grant
			req := ex.RequestCS{BaseMessage: dsnet.BaseMessage{From: en.Net.ID, To: st.coordinatorID, Type: "RequestCS"}, MutexID: st.mutexID}
			en.Net.Send(ctx, st.coordinatorID, req)
			st.waitingGrant = true
		}

	case "RequestCS":
		if !st.isCoordinator {
			return
		}
		var req ex.RequestCS
		if err := json.Unmarshal(event.Payload, &req); err != nil {
			return
		}
		handleCoordinatorRequest(ctx, en, st, req.From, req.MutexID)

	case "ReplyCS":
		if st.isCoordinator {
			return
		}
		var rep ex.ReplyCS
		if err := json.Unmarshal(event.Payload, &rep); err != nil {
			return
		}
		if !st.waitingGrant {
			return
		}
		if rep.Granted {
			st.waitingGrant = false
			doClientCS(ctx, en, st)
		} else {
			// Backoff and retry
			time.Sleep(100 * time.Millisecond)
			req := ex.RequestCS{BaseMessage: dsnet.BaseMessage{From: en.Net.ID, To: st.coordinatorID, Type: "RequestCS"}, MutexID: st.mutexID}
			en.Net.Send(ctx, st.coordinatorID, req)
		}

	case "ReleaseCS":
		if !st.isCoordinator {
			return
		}
		// client released; mark completion and maybe finish, else grant next
		st.inCS = false
		st.holder = ""
		st.completed[event.From] = true
		if len(st.completed) >= len(st.allNodes)-1 {
			res := ex.MutexResult{BaseMessage: dsnet.BaseMessage{From: en.Net.ID, To: "TESTER", Type: "MutexResult"}, MutexID: st.mutexID, NodeId: en.Net.ID, Success: true}
			en.Net.Send(ctx, "TESTER", res)
			return
		}
		grantNext(ctx, en, st)
	}
}

func handleCoordinatorRequest(ctx context.Context, en *MutexNode, st *state, from string, mutexID string) {
	// If free and no holder, grant immediately; else deny now and enqueue
	if !st.inCS && st.holder == "" {
		st.inCS = true
		st.holder = from
		rep := ex.ReplyCS{BaseMessage: dsnet.BaseMessage{From: en.Net.ID, To: from, Type: "ReplyCS"}, MutexID: mutexID, Granted: true}
		en.Net.Send(ctx, from, rep)
		return
	}
	// enqueue and send denial
	st.queue = append(st.queue, from)
	rep := ex.ReplyCS{BaseMessage: dsnet.BaseMessage{From: en.Net.ID, To: from, Type: "ReplyCS"}, MutexID: mutexID, Granted: false}
	en.Net.Send(ctx, from, rep)
}

func grantNext(ctx context.Context, en *MutexNode, st *state) {
	if st.inCS || len(st.queue) == 0 {
		return
	}
	next := st.queue[0]
	st.queue = st.queue[1:]
	st.inCS = true
	st.holder = next
	rep := ex.ReplyCS{BaseMessage: dsnet.BaseMessage{From: en.Net.ID, To: next, Type: "ReplyCS"}, MutexID: st.mutexID, Granted: true}
	en.Net.Send(ctx, next, rep)
}

func doClientCS(ctx context.Context, en *MutexNode, st *state) {
	// Simulate CS
	time.Sleep(time.Duration(st.workMillis) * time.Millisecond)
	// Release to coordinator
	rel := ex.ReleaseCS{BaseMessage: dsnet.BaseMessage{From: en.Net.ID, To: st.coordinatorID, Type: "ReleaseCS"}, MutexID: st.mutexID}
	en.Net.Send(ctx, st.coordinatorID, rel)
	// Report completion to tester
	res := ex.MutexResult{BaseMessage: dsnet.BaseMessage{From: en.Net.ID, To: "TESTER", Type: "MutexResult"}, MutexID: st.mutexID, NodeId: en.Net.ID, Success: true}
	en.Net.Send(ctx, "TESTER", res)
}

func doCoordinatorCS(ctx context.Context, en *MutexNode, st *state) {
	// Coordinator own CS
	time.Sleep(time.Duration(st.workMillis) * time.Millisecond)
	st.inCS = false
	st.holder = ""
	// Coordinator also reports its own completion
	res := ex.MutexResult{BaseMessage: dsnet.BaseMessage{From: en.Net.ID, To: "TESTER", Type: "MutexResult"}, MutexID: st.mutexID, NodeId: en.Net.ID, Success: true}
	en.Net.Send(ctx, "TESTER", res)
	grantNext(ctx, en, st)
}

func main() {
	time.Sleep(3 * time.Second)

	id := os.Getenv("NODE_ID")
	if id == "" {
		fmt.Println("NODE_ID environment variable not set")
		return
	}

	// Check if this node is the coordinator (has peers_json)
	peersJSON := os.Getenv("peers_json")
	if peersJSON != "" {
		// This is the coordinator
		fmt.Printf("Node %s starting as coordinator\\n", id)
		mutexNode := NewMutexNode(id, "")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		mutexNode.Run(ctx)
	} else {
		// This is a client node
		coordinatorID := os.Getenv("COORDINATOR_ID")
		if coordinatorID == "" {
			fmt.Println("Client node missing COORDINATOR_ID")
			return
		}
		fmt.Printf("Node %s starting as client (coordinator: %s)\\n", id, coordinatorID)
		mutexNode := NewMutexNode(id, coordinatorID)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		mutexNode.Run(ctx)
	}
}
