package mutex

import "github.com/distcodep7/dsnet/dsnet"

// ==========================================================
// 1. EXTERNAL TRIGGERS (The "Input")
// ==========================================================

type MutexTrigger struct {
	dsnet.BaseMessage        // Type: "MutexTrigger"
	MutexID        string `json:"mutex_id"`
	// Optional: target node to initiate on; if empty, tests can set To in BaseMessage
	WorkMillis     int    `json:"work_ms"` // Simulated CS work duration
}

// ==========================================================
// 2. INTERNAL PROTOCOL (The "Logic")
// ==========================================================

// RequestCS is sent between nodes to request access to the critical section.
type RequestCS struct {
	dsnet.BaseMessage 		// Type: "RequestCS"
	MutexID      	string `json:"mutex_id"`
}

// ReplyCS is the reply to a RequestCS.
type ReplyCS struct {
	dsnet.BaseMessage // Type: "ReplyCS"
	MutexID      	string `json:"mutex_id"`
	Granted	 		bool   `json:"granted"`
}

// ReleaseCS is broadcast when a node exits the critical section,
// so waiting nodes can re-evaluate and proceed.
type ReleaseCS struct {
	dsnet.BaseMessage // Type: "ReleaseCS"
	MutexID      	string `json:"mutex_id"`
}

// ==========================================================
// 3. EXTERNAL RESULTS (The "Output")
// ==========================================================

type MutexResult struct {
	dsnet.BaseMessage
	MutexID 		string `json:"mutex_id"`
	NodeId  		string `json:"node_id"` // Node that finished its CS execution
	Success 		bool   `json:"success"` // True if mutual exclusion and progress were maintained
}