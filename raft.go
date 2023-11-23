package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"sync"
	"time"
)
import "labrpc"

// import "bytes"
// import "encoding/gob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
const heartbeat = 30 * time.Millisecond

type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}
type LogEntry struct {
	Command interface{}
	Term    int
}
type ServerLogMap struct {
	ServerId int
	LogIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu                    sync.Mutex
	peers                 []*labrpc.ClientEnd
	persister             *Persister
	me                    int // index into peers[]
	currentTerm           int
	votedFor              int
	isNodeLeader          bool
	currentLeader         int
	hasvotedInCurTerm     bool
	electTimeOut          int
	heartbeatReceived     bool
	lastVotedterm         int
	msgLogs               []LogEntry
	commitIndex           int
	lastApplied           int
	nextIndex             []int //For leader
	matchIndex            []int //For leader
	applyChannel          chan ApplyMsg
	hasNewMsg             bool
	isWaitingForOldLeader bool
	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	term = rf.currentTerm
	isleader = rf.isNodeLeader
	// Your code here.
	//if isleader {
	//	println("server is leader", rf.me)
	//}

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.msgLogs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.msgLogs)
}

// example RequestVote RPC arguments structure.
type AppendEntryArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
	logSize      int
}

// example RequestVote RPC reply structure.
type AppendEntryReply struct {
	Term          int //CHange to capital letter to avoid not export field error
	Success       bool
	isMsgAppended bool
}

// example RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
type RequestVoteReply struct {
	Term        int //CHange to capital letter to avoid not export field error
	VoteGranted bool
	id          int
	isleader    bool
}

type TermVal struct {
	Term    int
	id      int
	msgLogs []LogEntry
}

type SuccessReply struct {
	Success bool
	msgLogs []LogEntry
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	//println(" Requesting vote from ", rf.me)
	//println(" current term =  ", rf.currentTerm)
	var vote_for bool
	vote_for = true
	reply.isleader = rf.isNodeLeader
	if args.Term <= rf.currentTerm || rf.me == args.CandidateId {
		vote_for = false
	}
	//if vote_for && (rf.votedFor == args.CandidateId || rf.votedFor == -1) {
	//	if args.LastLogIndex >= len(rf.msgLogs)-1 && args.LastLogTerm >= rf.msgLogs[len(rf.msgLogs)-1].Term {
	//		vote_for = vote_for && true
	//	} else {
	//		vote_for = false
	//	}
	//}
	//lastLogIndex := len(rf.msgLogs) - 1
	//if lastLogIndex > -1 {
	//	lastLogTerm := rf.msgLogs[lastLogIndex].Term
	//	if args.LastLogTerm < lastLogTerm {
	//		vote_for = false
	//	} else if args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex {
	//		vote_for = false
	//	}
	//}

	//if rf.isNodeLeader && args.Term > rf.currentTerm {
	//	rf.isNodeLeader = false
	//	if !rf.isNodeLeader {
	//		go func() {
	//			rf.triggerTimerForNode(rf.electTimeOut)
	//		}()
	//	}
	//}

	reply.VoteGranted = vote_for
	reply.id = rf.me
	if vote_for {
		reply.Term = args.Term
		rf.votedFor = args.CandidateId
		//println(" Voted for candidate ID ")
		//println(args.CandidateId)
	} else {
		reply.Term = rf.currentTerm
		//println(" Voted against candidate ID ")
		//println(args.CandidateId)
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
func (rf *Raft) sendAppendEntriesMsg(server int, args AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntryMsg", args, reply)
	return ok
}

//	func (rf *Raft) PrintCommitIndex(args TermVal, reply *SuccessReply) {
//		println(" id = ", rf.me)
//		println("commit Index = ", rf.commitIndex)
//	}
func (rf *Raft) AppendEntryMsg(args AppendEntryArgs, reply *AppendEntryReply) {
	rf.currentTerm = args.Term
	reply.Success = true
	reply.Term = args.Term

	//println("Inside server = ", rf.me)
	//println("args.logSize = ", len(args.Entries))

	if rf.commitIndex < args.LeaderCommit {
		var msgsLoggg = make([]LogEntry, 0)
		for i := 0; i < len(args.Entries); i++ {
			msgsLoggg = append(msgsLoggg, args.Entries[i])
		}
		rf.msgLogs = msgsLoggg
		for j := rf.commitIndex + 1; j <= args.LeaderCommit; j++ {
			rf.applyChannel <- ApplyMsg{Index: j, Command: rf.msgLogs[j].Command}
		}
		rf.commitIndex = args.LeaderCommit
	}

	//println("Putting inside channel ")

}
func (rf *Raft) updateMsgLogOfNewLeader(args TermVal, reply *SuccessReply) {
	rf.msgLogs = args.msgLogs
	reply.Success = true
	println(" Log updated")
}
func (rf *Raft) sendAppendEntries(server int, args AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}
func (rf *Raft) UpdateTerms(args TermVal, reply *SuccessReply) {
	if rf.isNodeLeader {
		println(" causing isnodeleader false for server = ", rf.me)
		var msgsLoggg = make([]LogEntry, 0)
		for i := 0; i < len(rf.msgLogs); i++ {
			msgsLoggg = append(msgsLoggg, rf.msgLogs[i])
		}
		reply.msgLogs = msgsLoggg
		rf.isNodeLeader = false
		rf.currentTerm = args.Term
		go func() {
			rf.triggerTimerForNode(rf.electTimeOut)
		}()
		reply.Success = true
	}
	reply.Success = false
}

func (rf *Raft) AppendEntry(args AppendEntryArgs, reply *AppendEntryReply) {
	//var success bool
	//success = true
	//println(" heartbeat to = ", rf.me)
	rf.heartbeatReceived = true
	if rf.me != args.LeaderId && rf.isNodeLeader && args.Term > rf.currentTerm {
		println(" hii is node leader false for ", rf.me)
		rf.isNodeLeader = false
	}
	if args.Term < rf.currentTerm {
		//println("Term <curTerm")
		reply.Success = false
		reply.Term = rf.currentTerm
	} else if args.Term >= rf.currentTerm {
		rf.currentLeader = args.LeaderId
		//if rf.isNodeLeader {
		//	rf.isNodeLeader = false
		//	go func() {
		//		rf.triggerTimerForNode(rf.electTimeOut)
		//	}()
		//}
		rf.currentTerm = args.Term
		reply.Success = true
		reply.Term = args.Term
		//println(" To follower = ", rf.me)
		//println(" rf commit index = ", rf.commitIndex)
		//println(" args.LeaderCommit = ", args.LeaderCommit)
		//println(" args.logSize = ", args.logSize)
		//if args.logSize != 0 {
		if len(rf.msgLogs)-1 >= args.PrevLogIndex && rf.msgLogs[args.PrevLogIndex].Term == args.PrevLogTerm {
			//append(rf.msgLogs, args.Entries[i])
			//println(" To follower = ", rf.me)
			//println(" rf commit index = ", rf.commitIndex)
			//println(" args.LeaderCommit = ", args.LeaderCommit)
			//var msgsLoggg = make([]LogEntry, 0)
			if len(args.Entries) > len(rf.msgLogs) {
				for i := args.PrevLogIndex + 1; i < len(args.Entries)-1; i++ {
					rf.msgLogs = append(rf.msgLogs, args.Entries[i])
				}
			}

			//rf.msgLogs = msgsLoggg
			if rf.commitIndex < args.LeaderCommit {
				for j := rf.commitIndex + 1; j <= args.LeaderCommit; j++ {
					rf.applyChannel <- ApplyMsg{Index: j, Command: rf.msgLogs[j].Command}
					rf.commitIndex = args.LeaderCommit
				}
			}
			rf.nextIndex[rf.me] = args.LeaderCommit + 1

		}
		//else if rf.commitIndex >= args.LeaderCommit {
		//	println(" To follower = ", rf.me)
		//	println(" rf commit index = ", rf.commitIndex)
		//	println(" args.LeaderCommit = ", args.LeaderCommit)
		//	var msgsLoggg = make([]LogEntry, 0)
		//	for i := 0; i < len(args.Entries); i++ {
		//		msgsLoggg = append(msgsLoggg, args.Entries[i])
		//	}
		//	rf.msgLogs = msgsLoggg
		//	for j := 0; j <= args.LeaderCommit; j++ {
		//		rf.applyChannel <- ApplyMsg{Index: j, Command: rf.msgLogs[j].Command}
		//	}
		//	rf.nextIndex[rf.me] = args.LeaderCommit + 1
		//	rf.commitIndex = args.LeaderCommit
		//}
		//}
	}
	//else if args.Term >= rf.currentTerm {
	//	//println(" Args term >= curterm")
	//	rf.currentTerm = args.Term
	//	reply.Success = true
	//	reply.Term = args.Term
	//
	//}

	//println(" AppendEntry from term = ", args.Term)
	//println(" current term =  ", rf.currentTerm)

	//reply.Term = args.Term
	//rf.heartbeatReceived = true
	//reply.isMsgAppended = false
	//if args.PrevLogIndex > -1 && args.PrevLogTerm > -1 && len(rf.msgLogs) > args.PrevLogIndex && rf.msgLogs[args.PrevLogIndex].Term != args.PrevLogTerm {
	//	success = false
	//	reply.Term = rf.currentTerm
	//}
	////println("args.LeaderCommit =", args.LeaderCommit)
	////println("rf.commitIndex =", rf.commitIndex)
	//if args.LeaderCommit > rf.commitIndex {
	//	reply.isMsgAppended = true
	//	rf.persist()
	//	for i := args.LeaderCommit + 1; i <= rf.commitIndex; i++ {
	//		entriess := LogEntry{args.Entries[i].Command, args.Entries[i].Term}
	//		rf.msgLogs = append(rf.msgLogs, &entriess)
	//		rf.commitIndex += 1
	//		DPrintf("[NotifyApplyCh] me:%d push to applyCh, Index:%v Command:%v", rf.me, i, rf.msgLogs[i].Command)
	//		rf.applyChannel <- ApplyMsg{Index: i, Command: rf.msgLogs[i].Command}
	//	}
	//} else if args.LeaderCommit < rf.commitIndex {
	//
	//	for i := 0; i <= rf.commitIndex; i++ {
	//
	//	}
	//	rf.commitIndex = args.LeaderCommit
	//}
	//
	//
	//reply.Success = success
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := rf.isNodeLeader
	if rf.isNodeLeader {
		//println(" rf.isNodeLeader = ", rf.isNodeLeader)
		//println(" server id = ", rf.me)
		//println("rf.nextIndex[rf.me] = ", rf.nextIndex[rf.me])
		//err_msg := ""
		fmt.Printf("commit index=%v server id=%v command =  %v", rf.nextIndex[rf.me], rf.me, command)
		//println(" command = ", command)
		//if len(err_msg) {
		//	print
		//}
		rf.hasNewMsg = true
		index = rf.nextIndex[rf.me]
		term = rf.currentTerm
		rf.msgLogs = append(rf.msgLogs, LogEntry{command, term})
		//rf.applyChannel <- ApplyMsg{Index: len(rf.msgLogs) - 1, Command: rf.msgLogs[len(rf.msgLogs)-1].Command}
		rf.matchIndex[rf.me] = rf.nextIndex[rf.me]
		//rf.commitIndex = len(rf.msgLogs) - 1
		//rf.nextIndex[rf.me] = 1
		//rf.commitIndex += 1
		//println(" rf.commitIndex = ", rf.commitIndex)
		//println("rf.msgLogs", rf.msgLogs)
		//println("len of rf.msgLogs", len(rf.msgLogs))
		rf.persist()
		//prevlogindex := rf.nextIndex[rf.me] - 1
		//prevlogTerm := rf.msgLogs[prevlogindex].Term
		rf.nextIndex[rf.me] += 1

		//for idx, _ := range rf.peers {
		//println(" Appending to peers")

		//}

	}
	//println(" index returned to sclient = ", index)
	return index, term, isLeader
}

// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.hasvotedInCurTerm = false
	rf.currentTerm = 0
	rf.isNodeLeader = false
	rf.votedFor = -1
	rf.heartbeatReceived = false
	// Your initialization code here.
	rf.electTimeOut = electionTimeOut(rf.me)
	logEntry := LogEntry{}
	logEntry.Term = -1
	logEntry.Command = -1
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.msgLogs = make([]LogEntry, 0)
	rf.msgLogs = append(rf.msgLogs, logEntry)
	//println(" len(rf.msgLogs) = ", len(rf.msgLogs))
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyChannel = applyCh
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = 1
	}
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex[i] = 0
	}
	//rf.matchIndex = make([]*ServerLogMap, len(peers))

	go func() {
		rf.triggerTimerForNode(rf.electTimeOut)
	}()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
func electionTimeOut(id int) int {
	var electiontimeout int
	electiontimeout = (id + 1) * 150
	println(" timeoput value = ", electiontimeout)
	return electiontimeout
}
func (rf *Raft) triggerTimerForNode(timeoutVal int) {
	var count int
	for !rf.isNodeLeader {
		var dur time.Duration = time.Duration(timeoutVal) * time.Millisecond
		time.Sleep(dur)
		//if rf.isNodeLeader {
		//	break
		//}
		if !rf.isNodeLeader {
			if rf.heartbeatReceived {
				rf.heartbeatReceived = false
			} else {
				//rf.currentTerm = rf.currentTerm + 1
				//println(" Term = ", rf.currentTerm)
				//println(rf.currentTerm)
				lastLogIndex := len(rf.msgLogs) - 1
				var lastLogeTerm int
				//if lastLogIndex >= 0 {
				lastLogeTerm = rf.msgLogs[lastLogIndex].Term
				//} else {
				//	lastLogeTerm = -1
				//}
				Args := &RequestVoteArgs{rf.currentTerm + 1, rf.me, lastLogIndex, lastLogeTerm}
				//Args.term = rf.currentTerm
				//Args.candidateId = rf.me
				//println(" Sending request vote from id ")
				//println(rf.me)
				var votedForCnt int
				votedForCnt = 1
				rf.hasvotedInCurTerm = true
				var votedAgainstCnt int
				votedAgainstCnt = -1
				var leaderCnt int
				leaderCnt = 0
				//println(" FORRRRR ", rf.me)
				for index, _ := range rf.peers {
					var reply RequestVoteReply
					//println(" before senging reply.VoteGranted = ", reply.VoteGranted)
					ok := rf.sendRequestVote(index, *Args, &reply)
					//if ok {
					//println(" reply.VoteGranted = ", reply.VoteGranted)
					//}
					//println("by = ", reply.id)
					if ok {
						if reply.isleader {
							leaderCnt += 1
						}
						if reply.VoteGranted {
							votedForCnt += 1
						} else {
							votedAgainstCnt += 1
						}
					}
				}
				var isElectedAsLeader bool
				//println(" Voted For = ")
				//println(votedForCnt)
				//println(" Voted againt = ")
				//println(votedAgainstCnt)
				isElectedAsLeader = votedForCnt > votedAgainstCnt && !(votedForCnt == 1 && votedAgainstCnt == -1)
				//println(" isElectedAsLeader = ", isElectedAsLeader)
				//print(isElectedAsLeader)
				//print(reply)
				if isElectedAsLeader || leaderCnt == 0 {
					rf.currentTerm += 1
					//rf.currentLeader
					rf.isWaitingForOldLeader = true
					for index, _ := range rf.peers {
						Args := &TermVal{rf.currentTerm, rf.me, make([]LogEntry, 0)}
						var successs SuccessReply
						rf.peers[index].Call("Raft.UpdateTerms", *Args, &successs)
						//if successs.Success {
						//	var msgsLoggg = make([]LogEntry, 0)
						//	for i := 0; i < len(successs.msgLogs); i++ {
						//		msgsLoggg = append(msgsLoggg, successs.msgLogs[i])
						//	}
						//	//for index, _ := range rf.peers {
						//	rf.msgLogs = msgsLoggg
						//	for j := 0; j <= len(successs.msgLogs); j++ {
						//
						//		rf.applyChannel <- ApplyMsg{Index: j, Command: rf.msgLogs[j].Command}
						//		fmt.Printf("Inserting into =%v index=%v command =  %v", rf.me, j, rf.msgLogs[j].Command)
						//	}
						//	//ok := rf.peers[index].Call("Raft.updateMsgLogOfNewLeader", args, reply)
						//	//reply.Success = ok
						//	//}
						//}
					}
					rf.isWaitingForOldLeader = false
					rf.isNodeLeader = true
					go func() {
						rf.generateHeartBeat()
					}()
				}
			}
		}
		count++
	}
}
func (rf *Raft) generateHeartBeat() {
	//var isSuccess bool
	//isSuccess = true
	for rf.isNodeLeader {
		//println(" heeartbeat for server = ", rf.me)
		//println(" starting heartbeat from id")
		//println(rf.me)
		var dur time.Duration = time.Duration(10) * time.Millisecond
		time.Sleep(dur)
		//var count int
		//count = 0
		var count int
		count = 0
		for index, _ := range rf.peers {
			var isReplySuccess bool
			//println("to peer ", index)
			//var reply AppendEntryReply
			//println("index = ", index)
			prevlogindex := rf.nextIndex[rf.me] - 1
			prevlogTerm := rf.msgLogs[prevlogindex].Term
			//if rf.nextIndex[index] > 0 {
			//	PrevLogIndex = rf.nextIndex[index] - 1
			//	PrevLogTerm = rf.msgLogs[PrevLogIndex].Term
			//}

			//Entries :=      []*LogEntry
			//LeaderCommit := rf.commitIndex
			LeaderCommit := rf.commitIndex
			var reply AppendEntryReply
			//var entries []LogEntry
			var size int
			size = rf.nextIndex[rf.me]
			//println(" rf.nextIndex[rf.me] size", size)
			//entries = append(entries, LogEntry{command, term})
			//Args := &AppendEntryArgs{rf.currentTerm, rf.me, prevlogindex, prevlogTerm, rf.msgLogs, LeaderCommit, size}
			//isSuccess = rf.peers[idx].Call("Raft.AppendEntryMsg", &Args, reply)
			//println(" calling heartbeat from server = ", rf.me)
			Args := &AppendEntryArgs{rf.currentTerm, rf.me, prevlogindex, prevlogTerm, rf.msgLogs, LeaderCommit, size}
			ok := rf.sendAppendEntries(index, *Args, &reply)
			isReplySuccess = reply.Success
			if isReplySuccess && ok {
				count += 1
			} else if ok && !isReplySuccess {
				//println(" Inside breakkkkk")
				rf.currentTerm = reply.Term
				rf.isNodeLeader = false
				break
			}
			//}

		}
		//println(" count == ", count)
		if count > 0 {
			//println(" server number = ", rf.me)
			//println(" Setting commit index for leader = ", rf.matchIndex[rf.me])
			if len(rf.msgLogs) > rf.commitIndex+1 {
				//println(" Inside Required")
				for j := rf.commitIndex + 1; j < len(rf.msgLogs); j++ {
					rf.applyChannel <- ApplyMsg{Index: j, Command: rf.msgLogs[j].Command}
				}
			}

			rf.commitIndex = len(rf.msgLogs) - 1
		}
		//println(" count = ", count)
		//if count > len(rf.peers)/2 {
		//	rf.commitIndex += 1
		//}
		if !rf.isNodeLeader {
			go func() {
				rf.triggerTimerForNode(rf.electTimeOut)
			}()
		}
	}
}
