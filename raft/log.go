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
	"fmt"
	golog "log"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	offset uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	// init by storage
	offset, _ := storage.FirstIndex()
	stabled, _ := storage.LastIndex()
	// recover first to stabled into memory
	ents, _ := storage.Entries(offset, stabled+1)

	return &RaftLog{
		storage:         storage,
		stabled:         stabled,
		offset:          offset,
		applied:         offset - 1,
		entries:         ents,
		pendingSnapshot: nil,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return nil
	}

	return l.entries[l.stabled-l.offset+1:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	if l.applied == l.committed {
		return nil
	}

	return l.entries[l.applied-l.offset+1 : l.committed-l.offset+1]
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		idx, _ := l.storage.LastIndex()
		return idx
	}

	return l.entries[len(l.entries)-1].GetIndex()
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if i > l.LastIndex() {
		return 0, nil
	}

	// get from unstabled
	if len(l.entries) > 0 && i >= l.offset {
		return l.entries[i-l.offset].Term, nil
	}

	term, err := l.storage.Term(i)
	if err != nil {
		return 0, err
	}

	return term, nil
}

func (l *RaftLog) leaderAppend(entries ...pb.Entry) {
	if len(entries) <= 0 {
		return
	}
	startIdx := entries[0].GetIndex()
	if startIdx < l.committed {
		golog.Panicln("the log should be append-only")
	}
	l.entries = append(l.entries, entries...)
}

func (l *RaftLog) maybeAppend(entries ...pb.Entry) {
	// truncate current unstabled
	if len(entries) == 0 {
		return
	}
	idx := entries[0].GetIndex()
	if idx > l.stabled && idx < uint64(len(l.entries))+l.offset {
		l.entries = append(l.entries[:idx-l.offset], entries...)
		return
	} else if idx > l.LastIndex() {
		l.entries = append(l.entries, entries...)
	} else {
		golog.Panicln("invalid log append operation")
	}
}

func (l *RaftLog) String() string {
	return fmt.Sprintf("commited=%d, applied=%d, entry offset=%d, len(l.entries)=%d", l.committed, l.applied, l.offset, len(l.entries))
}
