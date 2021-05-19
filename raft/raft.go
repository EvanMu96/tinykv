// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"log"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

type TermCheckResult uint8

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

const (
	CurrentTerm TermCheckResult = iota
	OlderTermDiscard
	NewerTermUpdate
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

// some fields that cannot be vacant
func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}

	r := Raft{
		id:               c.ID,
		Prs:              make(map[uint64]*Progress),
		State:            StateFollower,
		Term:             1,
		votes:            make(map[uint64]bool),
		RaftLog:          newLog(c.Storage),
		msgs:             make([]pb.Message, 0),
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		leadTransferee:   0,
	}

	for _, pr_id := range c.peers {
		r.Prs[pr_id] = &Progress{}
	}

	return &r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	}

	r.msgs = append(r.msgs, msg)

	return false
}

func (r *Raft) sendRequestVote() {

	index := r.RaftLog.LastIndex()
	term, err := r.RaftLog.Term(index)

	if err != nil {
		return
	}

	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		From:    r.id,
		Term:    r.Term,
		LogTerm: term,
		Index:   index,
	}

	r.msgs = append(r.msgs, msg)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower, StateCandidate:
		r.electionElapsed++
		if r.electionElapsed >= r.electionTimeout {
			r.becomeCandidate()
		}
	case StateLeader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.broadcastHeartBeatMsg()
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.Lead = lead
	r.Term = term
	r.State = StateFollower
	log.Printf("raft: raft instance %d switch to folower state, term: %d", r.id, r.Term)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	if len(r.Prs) == 0 {
		r.becomeLeader()
		return
	}
	r.sendRequestVote()
	// vote to self
	r.votes[r.id] = true
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	if r.State != StateCandidate {
		return
	}
	r.State = StateLeader
	r.Lead = r.id
	r.electionElapsed = 0
	r.heartbeatTimeout = 0
	// clear votes
	r.votes = make(map[uint64]bool)
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		switch m.MsgType {
		case pb.MessageType_MsgAppend:
			termCheck := r.checkMsgTerm(m.Term)
			switch termCheck {
			case OlderTermDiscard:
				return errors.New("out of date")
			case NewerTermUpdate:
				r.becomeFollower(m.GetTerm(), m.GetFrom())
			}
			r.handleAppendEntries(m)
		case pb.MessageType_MsgBeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgRequestVote:
			if m.Index < r.RaftLog.LastIndex() {
				return ErrProposalDropped
			}

			term, err := r.RaftLog.Term(m.Index)

			if err != nil {
				return ErrProposalDropped
			}

			if term <= r.Term {
				return ErrProposalDropped
			}

			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				From:    r.id,
				To:      m.From,
				Reject:  false,
			})
		}
	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgAppend:
			termCheck := r.checkMsgTerm(m.Term)
			switch termCheck {
			case OlderTermDiscard:
				return errors.New("out of date")
			case NewerTermUpdate:
				r.becomeFollower(m.GetTerm(), m.GetFrom())
			}
			r.handleAppendEntries(m)
		case pb.MessageType_MsgRequestVoteResponse:
		}
	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgAppend:
			termCheck := r.checkMsgTerm(m.Term)
			switch termCheck {
			case OlderTermDiscard:
				return errors.New("out of date")
			case NewerTermUpdate:
				r.becomeFollower(m.GetTerm(), m.GetFrom())
				r.handleAppendEntries(m)
			default:
				return nil
			}
		case pb.MessageType_MsgRequestVote:

		}
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if m.From != r.Lead || m.To != r.id {
		return
	}

	if m.Term < r.Term {
		return
	}

	if m.Term > r.Term {
		r.becomeFollower(m.GetFrom(), m.GetFrom())
	}

	for idx := range m.Entries {
		r.RaftLog.entries = append(r.RaftLog.entries, *m.Entries[idx])
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if m.From != r.Lead || m.To != r.id {
		return
	}

	r.electionElapsed = 0
}

func (r *Raft) reset() {
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.votes = make(map[uint64]bool)
	r.Lead = 0
	r.Term = 1
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

func (r *Raft) checkMsgTerm(term uint64) TermCheckResult {
	if r.Term == term {
		return CurrentTerm
	}

	if r.Term < term {
		return NewerTermUpdate
	}

	return OlderTermDiscard
}

type broadcastMsgFunc func()

func (r *Raft) broadcastHeartBeatMsg() {
	for k := range r.Prs {
		if k == r.id {
			continue
		}
		msg := pb.Message{
			MsgType: pb.MessageType_MsgHeartbeat,
			To:      k,
			From:    r.id,
			Term:    r.Term,
		}

		r.msgs = append(r.msgs, msg)
	}
}
