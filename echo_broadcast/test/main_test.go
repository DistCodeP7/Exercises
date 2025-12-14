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

var (
	Peers []string
	ID    string
	WM    *wrapper.WrapperManager
	ctx   context.Context
)

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

func TestEchoTwoNodes(t *testing.T) {
	disttest.Wrap(t, func(t *testing.T) {
		go controller.Serve(controller.TestConfig{})
		time.Sleep(2 * time.Second)

		WM.StartAll(ctx)
		time.Sleep(3 * time.Second)

		tester, err := dsnet.NewNode("TESTER", "localhost:50051")
		if err != nil {
			t.Fatalf("Failed to create TESTER node: %v", err)
		}
		defer tester.Close()

		trigger := shared.SendTrigger{
			BaseMessage: dsnet.BaseMessage{
				From: "TESTER",
				To:   Peers[0],
				Type: "SendTrigger",
			},
			EchoID:  "TEST_001",
			Content: "Hello, DSNet!",
		}
		tester.Send(ctx, Peers[0], trigger)
		log.Printf("Sent SendTrigger %s to %s", trigger.Content, Peers[0])

		timeout := time.After(30 * time.Second)
		for {
			select {
			case event := <-tester.Inbound:
				if event.Type == "ReplyReceived" {
					var result shared.ReplyReceived
					json.Unmarshal(event.Payload, &result)

					if result.EchoID == "TEST_001" {
						log.Printf("✅ TEST PASSED: Received EchoResponse from %s", Peers[0])
						return
					}
				}
			case <-timeout:
				t.Fatal("Timed out waiting for ReplyReceived")
			}
		}
	})
}
