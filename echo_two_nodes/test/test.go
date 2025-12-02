package main

import (
	"context"
	"distcode/echo"
	"encoding/json"
	"log"
	"time"

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

	trigger := echo.SendTrigger{
		BaseMessage: dsnet.BaseMessage{
			From: "TESTER",
			To:   "replica1",
			Type: "SendTrigger",
		},
		EchoID:  "TEST_001",
		Content: "Hello, DSNet!",
	}

	log.Println("Sending SendTrigger...")
	tester.Send(ctx, "replica1", trigger)

	// 2. Wait for the specific Result
	timeout := time.After(10 * time.Second)
	for {
		select {
		case event := <-tester.Inbound:
			if event.Type == "ReplyReceived" {
				var result echo.ReplyReceived
				json.Unmarshal(event.Payload, &result)

				if result.EchoID == "TEST_001" {
					log.Println("✅ TEST PASSED: Received EchoResponse from replica2")
					return
				}
			}
		case <-timeout:
			log.Fatal("❌ TEST FAILED: Timed out waiting for ReplyReceived")
		}
	}
}
