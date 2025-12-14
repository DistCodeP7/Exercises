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

var totalNodes int
var Peers []string
var id string

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
	n, err := dsnet.NewNode(id, "test-container:50051")
	if err != nil {
		log.Fatalf("Failed to create node %s: %v", id, err)
	}
	return &MutexNode{Net: n, CoordinatorID: coordinatorID}
}

func main() {
	id = os.Getenv("ID")
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if id == Peers[0] {
		log.Printf("%s starting as coordinator\n", id)
		mutexNode := NewMutexNode(id, "")
		mutexNode.Run(ctx)
	} else {
		mutexNode := NewMutexNode(id, Peers[0])
		mutexNode.Run(ctx)
	}
}

func (en *MutexNode) Run(ctx context.Context) {
	defer en.Net.Close()
	isCoordinator := en.CoordinatorID == ""
	coordinatorID := en.CoordinatorID
	if isCoordinator {
		coordinatorID = en.Net.ID
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
		var trig shared.MutexTrigger
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
			st.allNodes = Peers
			for _, id := range st.allNodes {
				if id == en.Net.ID {
					continue
				}
				t := shared.MutexTrigger{BaseMessage: dsnet.BaseMessage{From: en.Net.ID, To: id, Type: "MutexTrigger"}, MutexID: st.mutexID, WorkMillis: st.workMillis}
				en.Net.Send(ctx, id, t)
			}
			st.inCS = true
			doCoordinatorCS(ctx, en, st)
		} else {
			req := shared.RequestCS{BaseMessage: dsnet.BaseMessage{From: en.Net.ID, To: st.coordinatorID, Type: "RequestCS"}, MutexID: st.mutexID}
			en.Net.Send(ctx, st.coordinatorID, req)
			st.waitingGrant = true
		}

	case "RequestCS":
		if !st.isCoordinator {
			return
		}
		var req shared.RequestCS
		if err := json.Unmarshal(event.Payload, &req); err != nil {
			return
		}
		handleCoordinatorRequest(ctx, en, st, req.From, req.MutexID)

	case "ReplyCS":
		if st.isCoordinator {
			return
		}
		var rep shared.ReplyCS
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
			req := shared.RequestCS{BaseMessage: dsnet.BaseMessage{From: en.Net.ID, To: st.coordinatorID, Type: "RequestCS"}, MutexID: st.mutexID}
			en.Net.Send(ctx, st.coordinatorID, req)
		}

	case "ReleaseCS":
		if !st.isCoordinator {
			return
		}
		st.inCS = false
		st.holder = ""
		st.completed[event.From] = true
		log.Printf("[Coordinator] Node %s completed CS. Total completed: %d/%d\n", event.From, len(st.completed), len(st.allNodes))

		if len(st.completed) >= len(st.allNodes) {
			log.Printf("[Coordinator] All nodes completed. Exiting.\n")
			time.Sleep(100 * time.Millisecond)
			os.Exit(0)
		}
		grantNext(ctx, en, st)
	}
}

func handleCoordinatorRequest(ctx context.Context, en *MutexNode, st *state, from string, mutexID string) {
	if !st.inCS && st.holder == "" {
		st.inCS = true
		st.holder = from
		rep := shared.ReplyCS{BaseMessage: dsnet.BaseMessage{From: en.Net.ID, To: from, Type: "ReplyCS"}, MutexID: mutexID, Granted: true}
		en.Net.Send(ctx, from, rep)
		return
	}

	log.Printf("[Coordinator] Node %s requested CS but currently held by %s. Queuing request.\n", from, st.holder)
	st.queue = append(st.queue, from)
	rep := shared.ReplyCS{BaseMessage: dsnet.BaseMessage{From: en.Net.ID, To: from, Type: "ReplyCS"}, MutexID: mutexID, Granted: false}
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
	rep := shared.ReplyCS{BaseMessage: dsnet.BaseMessage{From: en.Net.ID, To: next, Type: "ReplyCS"}, MutexID: st.mutexID, Granted: true}
	en.Net.Send(ctx, next, rep)
}

func doClientCS(ctx context.Context, en *MutexNode, st *state) {
	log.Printf("[%s] Entering CS\n", en.Net.ID)
	time.Sleep(time.Duration(st.workMillis) * time.Millisecond)
	log.Printf("[%s] Exiting CS\n", en.Net.ID)

	rel := shared.ReleaseCS{BaseMessage: dsnet.BaseMessage{From: en.Net.ID, To: st.coordinatorID, Type: "ReleaseCS"}, MutexID: st.mutexID}
	en.Net.Send(ctx, st.coordinatorID, rel)

	res := shared.MutexResult{BaseMessage: dsnet.BaseMessage{From: en.Net.ID, To: "TESTER", Type: "MutexResult"}, MutexID: st.mutexID, NodeId: en.Net.ID, Success: true}
	en.Net.Send(ctx, "TESTER", res)
}

func doCoordinatorCS(ctx context.Context, en *MutexNode, st *state) {
	log.Printf("[Coordinator] Entering CS\n")
	time.Sleep(time.Duration(st.workMillis) * time.Millisecond)
	st.inCS = false
	st.holder = ""

	st.completed[en.Net.ID] = true
	log.Printf("[Coordinator] Exited CS. Completed: %d/%d\n", len(st.completed), len(st.allNodes))

	res := shared.MutexResult{BaseMessage: dsnet.BaseMessage{From: en.Net.ID, To: "TESTER", Type: "MutexResult"}, MutexID: st.mutexID, NodeId: en.Net.ID, Success: true}
	en.Net.Send(ctx, "TESTER", res)

	if len(st.completed) >= len(st.allNodes) {
		log.Printf("[Coordinator] All nodes completed. Exiting.\n")
		time.Sleep(100 * time.Millisecond) // Give time for message delivery
		os.Exit(0)
	}
	grantNext(ctx, en, st)
}
