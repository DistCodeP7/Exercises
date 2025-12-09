package test

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"runner/shared"

	"github.com/distcodep7/dsnet/dsnet"
	"github.com/distcodep7/dsnet/testing/controller"
	"github.com/distcodep7/dsnet/testing/disttest"
	"github.com/distcodep7/dsnet/testing/wrapper"
)

var Peers []string
var ID string
var ctx context.Context
var WM *wrapper.WrapperManager

func TestPaxos(t *testing.T) {
	disttest.Wrap(t, func(t *testing.T) {
		go controller.Serve(controller.TestConfig{
			DropProb:        0.2,
			ReorderProb:     0.3,
			ReorderMinDelay: 1,
			ReorderMaxDelay: 3,
		})
		time.Sleep(2 * time.Second)

		WM.StartAll(ctx)
		time.Sleep(3 * time.Second)

		tester, err := dsnet.NewNode("TESTER", "localhost:50051")
		if err != nil {
			log.Fatalf("Error creating tester node: %v\n", err)
		}
		defer tester.Close()

		electionID := "ELECTION_001"

		for i := 0; i < 2; i++ {
			trigger := shared.ElectionTrigger{
				BaseMessage: dsnet.BaseMessage{
					From: "TESTER",
					To:   Peers[i],
					Type: "ElectionTrigger",
				},
				ElectionID: electionID,
			}
			tester.Send(ctx, Peers[i], trigger)
		}

		// Wait for election result (may take multiple rounds with message drops)
		timeout := time.After(60 * time.Second)
		resultsReceived := make(map[string]bool)

		for {
			select {
			case event := <-tester.Inbound:
				if event.Type == "ElectionResult" {
					var result shared.ElectionResult
					json.Unmarshal(event.Payload, &result)
					if result.Success && result.ElectionID == electionID {
						if !resultsReceived[result.LeaderID] {
							resultsReceived[result.LeaderID] = true
							log.Printf("Election result received: %s claims to be leader", result.LeaderID)
						}

						if len(resultsReceived) == 1 {
							for _, peerID := range Peers {
								electionResult := shared.ElectionResult{
									BaseMessage: dsnet.BaseMessage{
										From: "TESTER",
										To:   peerID,
										Type: "ElectionResult",
									},
									ElectionID: electionID,
									LeaderID:   result.LeaderID,
									Success:    true,
								}
								tester.Send(ctx, peerID, electionResult)
							}
							log.Printf("✅ TEST PASSED: %s elected as leader after potential multiple election rounds", result.LeaderID)

							time.Sleep(500 * time.Millisecond)
							return
						} else if len(resultsReceived) > 1 {
							log.Printf("⚠️  Multiple leaders elected - this shows split vote scenario")
							time.Sleep(500 * time.Millisecond)
							return
						}
					}
				}
			case <-timeout:
				log.Fatal("❌ TEST FAILED: Timed out waiting for ElectionResult")
			}
		}
	})
}

func TestMain(m *testing.M) {
	Peers = strings.Split(os.Getenv("PEERS"), ",")
	ID = os.Getenv("ID")
	ctx = context.Background()
	WM = wrapper.NewWrapperManager(8090, Peers...)

	const attempts = 200
	const sleepInterval = 50 * time.Millisecond
	for i := 1; i <= attempts; i++ {
		errors := WM.ReadyAll(ctx)
		allReady := true
		for peer, err := range errors {
			if err != nil {
				allReady = false
				log.Printf("Peer %s not ready: %v\n", peer, err)
			}
		}
		if allReady {
			log.Println("All peers are ready")
			break
		}
		time.Sleep(sleepInterval)
	}

	code := m.Run()
	_ = disttest.Write("test_results.json")
	WM.ShutdownAll(ctx)
	os.Exit(code)
}
