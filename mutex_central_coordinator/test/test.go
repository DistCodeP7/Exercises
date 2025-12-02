package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"distcode/mutex"

	"github.com/distcodep7/dsnet/dsnet"
	controller "github.com/distcodep7/dsnet/testing/controller"
)

func main() {
	go controller.Serve()
	time.Sleep(2 * time.Second)

	tester, err := dsnet.NewNode("TESTER", "localhost:50051")
	if err != nil {
		fmt.Printf("Error creating tester node: %v\\n", err)
		return
	}
	defer tester.Close()

	peersJSON := os.Getenv("peers_json")
	var peers []string
	if err := json.Unmarshal([]byte(peersJSON), &peers); err != nil {
		fmt.Printf("invalid peersJSON: %v\\n", err)
		return
	}

	numNodes := len(peers)
	fmt.Printf("Test starting with %d nodes: %v\\n", numNodes, peers)

	time.Sleep(5 * time.Second) // Wait for all nodes to be ready

	trigger := mutex.MutexTrigger{
		BaseMessage: dsnet.BaseMessage{From: "TESTER", To: peers[0], Type: "MutexTrigger"},
		MutexID:     "TEST_MUTEX_001",
		WorkMillis:  300,
	}
	log.Printf("Sending MutexTrigger to %s...\\n", peers[0])
	tester.Send(context.Background(), peers[0], trigger)

	received := map[string]bool{}
	expected := map[string]bool{}
	for _, peer := range peers {
		expected[peer] = true
	}

	timeout := time.After(30 * time.Second)
	for {
		if len(received) >= len(expected) {
			log.Println("✅ TEST PASSED: All nodes completed critical section")
			return
		}
		select {
		case event := <-tester.Inbound:
			if event.Type == "MutexResult" {
				var result mutex.MutexResult
				json.Unmarshal(event.Payload, &result)
				if result.Success && expected[result.NodeId] && result.MutexID == "TEST_MUTEX_001" {
					received[result.NodeId] = true
					log.Printf("Node %s completed CS (%d/%d)", result.NodeId, len(received), len(expected))
				}
			}
		case <-timeout:
			// Identify missing nodes for better error output
			missing := []string{}
			for n := range expected {
				if !received[n] {
					missing = append(missing, n)
				}
			}
			log.Fatalf("❌ TEST FAILED: Timed out waiting for MutexResult from: %v", missing)
		}
	}
}
