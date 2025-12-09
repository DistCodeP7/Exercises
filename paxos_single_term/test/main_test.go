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
		go controller.Serve(controller.TestConfig{})
		time.Sleep(2 * time.Second)

		WM.StartAll(ctx)
		time.Sleep(3 * time.Second)

		tester, err := dsnet.NewNode("TESTER", "localhost:50051")
		if err != nil {
			log.Fatalf("Error creating tester node: %v\n", err)
		}
		defer tester.Close()

		trigger := shared.ElectionTrigger{
			BaseMessage: dsnet.BaseMessage{
				From: "TESTER",
				To:   Peers[0],
				Type: "ElectionTrigger",
			},
			ElectionID: "TEST_001",
		}
		tester.Send(ctx, Peers[0], trigger)

		timeout := time.After(30 * time.Second)
		for {
			select {
			case event := <-tester.Inbound:
				if event.Type == "ElectionResult" {
					var result shared.ElectionResult
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
