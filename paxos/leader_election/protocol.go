package leader_election

import "github.com/distcodep7/dsnet/dsnet"

// ==========================================================
// 1. EXTERNAL TRIGGERS (The "Input")
// ==========================================================

// ElectionTrigger is sent by the Tester to start the exercise.
// The student must listen for type: "ElectionTrigger"
type ElectionTrigger struct {
	dsnet.BaseMessage        // Type: "ElectionTrigger"
	ElectionID        string `json:"election_id"`
}

// ==========================================================
// 2. INTERNAL PROTOCOL (The "Logic")
// ==========================================================

// RequestVote is sent between nodes to ask for leadership.
type RequestVote struct {
	dsnet.BaseMessage     // Type: "RequestVote"
	Term              int `json:"term"`
	LastLogIndex      int `json:"last_log_index"`
}

// VoteResponse is the reply to a RequestVote.
type VoteResponse struct {
	dsnet.BaseMessage      // Type: "VoteResponse"
	Term              int  `json:"term"`
	Granted           bool `json:"granted"`
}

// ==========================================================
// 3. EXTERNAL RESULTS (The "Output")
// ==========================================================

type ElectionResult struct {
	dsnet.BaseMessage
	ElectionID string `json:"election_id"`
	Success    bool   `json:"success"`
	LeaderID   string `json:"leader_id"`
}
