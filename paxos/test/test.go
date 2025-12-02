package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"distcode/leader_election"

	"github.com/distcodep7/dsnet/dsnet"
	"github.com/distcodep7/dsnet/testing/controller"
)

func main() {
	go controller.Serve()
	time.Sleep(2 * time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tester, _ := dsnet.NewNode("TESTER", "localhost:50051")
	defer tester.Close()

	time.Sleep(5 * time.Second)

	// 1. Prepare the specific Trigger
	trigger := leader_election.ElectionTrigger{
		BaseMessage: dsnet.BaseMessage{
			From: "TESTER",
			To:   "replica1",
			Type: "ElectionTrigger", // Matches struct name
		},
		ElectionID: "TEST_001",
	}

	log.Println("Sending ElectionTrigger...")
	tester.Send(ctx, "replica1", trigger)

	// 2. Wait for the specific Result
	timeout := time.After(15 * time.Second)
	for {
		select {
		case event := <-tester.Inbound:
			if event.Type == "ElectionResult" {
				var result leader_election.ElectionResult
				json.Unmarshal(event.Payload, &result)

				if result.Success {
					log.Printf("✅ TEST PASSED: %s elected successfully", result.LeaderID)
					return
				}
			}
		case <-timeout:
			log.Fatal("❌ TEST FAILED: Timed out waiting for ElectionResult")
		}
	}
}
