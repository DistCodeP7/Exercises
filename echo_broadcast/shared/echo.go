package echo

import "github.com/distcodep7/dsnet/dsnet"

// ==========================================================
// 1. EXTERNAL TRIGGERS (The "Input")
// ==========================================================

// SendTrigger sends a trigger message to a single node to start the echo process.
type SendTrigger struct {
	dsnet.BaseMessage      // Type: "SendTrigger"
	EchoID 				string `json:"echo_id"`
	Content      		string `json:"content"`
}

// ==========================================================
// 2. INTERNAL PROTOCOL (The "Logic")
// ==========================================================

type EchoMessage struct {
	dsnet.BaseMessage     // Type: "EchoMessage"
	EchoID		 		string `json:"echo_id"`
	Content         	string `json:"content"`
}

// EchoResponse is sent from each receiving node back to the original sender.
type EchoResponse struct {
	dsnet.BaseMessage     // Type: "EchoResponse"
	EchoID		 		string `json:"echo_id"`
	Content         	string `json:"content"`
}

// ==========================================================
// 3. EXTERNAL RESULTS (The "Output")
// ==========================================================

// ReplyReceived is emitted *once*, by the original sender,
// after it has received an EchoResponse from every other node.
type ReplyReceived struct {
	dsnet.BaseMessage
	EchoID 				string `json:"echo_id"`
	Success 			bool `json:"success"`
}