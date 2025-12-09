package test

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"runner/shared"
	"strings"
	"testing"
	"time"

	"github.com/distcodep7/dsnet/dsnet"
	"github.com/distcodep7/dsnet/testing/controller"
	"github.com/distcodep7/dsnet/testing/disttest"
	"github.com/distcodep7/dsnet/testing/wrapper"
)

var Peers []string
var ID string
var ctx context.Context
var WM *wrapper.WrapperManager

func TestMutexCentralCoord(t *testing.T) {
	disttest.Wrap(t, func(t *testing.T) {
		go controller.Serve(controller.TestConfig{})
		time.Sleep(2 * time.Second)

		WM.StartAll(ctx)
		time.Sleep(3 * time.Second)

		tester, err := dsnet.NewNode("TESTER", "localhost:50051")
		if err != nil {
			log.Fatalf("Error creating tester node: %v\n", err)
		}
		defer tester.Close()

		trigger := shared.MutexTrigger{
			BaseMessage: dsnet.BaseMessage{From: "TESTER", To: Peers[0], Type: "MutexTrigger"},
			MutexID:     "TEST_MUTEX_001",
			WorkMillis:  300,
		}
		tester.Send(ctx, Peers[0], trigger)

		received := map[string]bool{}
		expected := map[string]bool{}
		for _, peer := range Peers {
			expected[peer] = true
		}

		timeout := time.After(30 * time.Second)
		for {
			select {
			case event := <-tester.Inbound:
				if event.Type == "MutexResult" {
					var result shared.MutexResult
					json.Unmarshal(event.Payload, &result)
					if result.Success && expected[result.NodeId] && result.MutexID == "TEST_MUTEX_001" {
						received[result.NodeId] = true
						log.Printf("Node %s completed CS (%d/%d)", result.NodeId, len(received), len(expected))

						if len(received) >= len(expected) {
							log.Println("✅ TEST PASSED: All nodes completed critical section")
							return
						}
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
