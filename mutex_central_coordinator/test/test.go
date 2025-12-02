package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"time"

	ex "distcode/mutex"

	"github.com/distcodep7/dsnet/dsnet"
	"github.com/distcodep7/dsnet/testing/wrapper"
)

func main() {
	numNodes := flag.Int("n", 3, "Number of nodes in the mutex system")
	flag.Parse()

	tester, _ := dsnet.NewNode("TESTER", "localhost:50051")

	trigger := ex.MutexTrigger{
		BaseMessage: dsnet.BaseMessage{ From: "TESTER", To: "N1", Type: "MutexTrigger" },
		MutexID:     "TEST_MUTEX_001",
		WorkMillis:  300,
	}
	log.Println("Sending MutexTrigger...")
	tester.Send(context.Background(), "N1", trigger)

	// Optional: use wrapper to reset a random node during the test (port 50001 assumed)
	wm := wrapper.NewWrapperManager(50001)
	go func() {
		time.Sleep(500 * time.Millisecond)
		_ = wm.Reset(context.Background(), wrapper.Alias("N2"))
	}()

	// Wait for MutexResult from all nodes
	received := map[string]bool{}
	expected := map[string]bool{}
	for i := 1; i <= *numNodes; i++ { expected[fmt.Sprintf("N%d", i)] = true }

	timeout := time.After(15 * time.Second)
	for {
		if len(received) >= len(expected) {
			log.Println("✅ TEST PASSED: All nodes completed critical section")
			return
		}
		select {
		case event := <-tester.Inbound:
			if event.Type == "MutexResult" {
				var result ex.MutexResult
				json.Unmarshal(event.Payload, &result)
				if result.Success && expected[result.NodeId] && result.MutexID == "TEST_MUTEX_001" {
					received[result.NodeId] = true
					log.Printf("Node %s completed CS (%d/%d)", result.NodeId, len(received), len(expected))
				}
			}
		case <-timeout:
			// Identify missing nodes for better error output
			missing := []string{}
			for n := range expected { if !received[n] { missing = append(missing, n) } }
			log.Fatalf("❌ TEST FAILED: Timed out waiting for MutexResult from: %v", missing)
		}
	}
}
