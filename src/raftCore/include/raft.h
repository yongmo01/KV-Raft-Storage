#ifndef RAFT_H
#define RAFT_H

#include <boost/any.hpp>
#include <boost/serialization/serialization.hpp>
#include <boost/serialization/string.hpp>
#include <boost/serialization/vector.hpp>

#include <chrono>
#include <cmath>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "ApplyMsg.h"
#include "Persister.h"
#include "config.h"
#include "metrics.h"
#include "monsoon.h"
#include "raftRpcUtil.h"
#include "util.h"

constexpr int Disconnected = 0;
constexpr int AppNormal = 1;

constexpr int Killed = 0;
constexpr int Voted = 1;
constexpr int Expire = 2;
constexpr int Normal = 3;

class Raft : public raftRpcProctoc::raftRpc {
 private:
  std::mutex m_mtx;
  std::vector<std::shared_ptr<RaftRpcUtil>> m_peers;
  std::shared_ptr<Persister> m_persister;
  int m_me;
  int m_currentTerm;
  int m_votedFor;
  std::vector<raftRpcProctoc::LogEntry> m_logs;

  int m_commitIndex;
  int m_lastApplied;

  std::vector<int> m_nextIndex;
  std::vector<int> m_matchIndex;
  enum Status { Follower, Candidate, Leader };
  Status m_status;

  std::shared_ptr<LockQueue<ApplyMsg>> applyChan;

  std::chrono::system_clock::time_point m_lastResetElectionTime;
  std::chrono::system_clock::time_point m_lastResetHearBeatTime;

  int m_lastSnapshotIncludeIndex;
  int m_lastSnapshotIncludeTerm;

  std::unique_ptr<monsoon::IOManager> m_ioManager = nullptr;

 public:
  void AppendEntries1(const raftRpcProctoc::AppendEntriesArgs* args, raftRpcProctoc::AppendEntriesReply* reply);
  void applierTicker();
  bool CondInstallSnapshot(int lastIncludedTerm, int lastIncludedIndex, std::string snapshot);
  void doElection();
  void doHeartBeat();
  void electionTimeOutTicker();
  std::vector<ApplyMsg> getApplyLogs();
  int getNewCommandIndex();
  void getPrevLogInfo(int server, int* preIndex, int* preTerm);
  void GetState(int* term, bool* isLeader);
  void InstallSnapshot(const raftRpcProctoc::InstallSnapshotRequest* args,
                       raftRpcProctoc::InstallSnapshotResponse* reply);
  void leaderHearBeatTicker();
  void leaderSendSnapShot(int server);
  void leaderUpdateCommitIndex();
  void appendNoopEntryLocked();
  bool matchLog(int logIndex, int logTerm);
  void persist();
  void RequestVote(const raftRpcProctoc::RequestVoteArgs* args, raftRpcProctoc::RequestVoteReply* reply);
  bool UpToDate(int index, int term);
  int getLastLogIndex();
  int getLastLogTerm();
  void getLastLogIndexAndTerm(int* lastLogIndex, int* lastLogTerm);
  int getLogTermFromLogIndex(int logIndex);
  int GetRaftStateSize();
  int getSlicesIndexFromLogIndex(int logIndex);

  bool sendRequestVote(int server, std::shared_ptr<raftRpcProctoc::RequestVoteArgs> args,
                       std::shared_ptr<raftRpcProctoc::RequestVoteReply> reply, std::shared_ptr<int> votedNum);
  bool sendAppendEntries(int server, std::shared_ptr<raftRpcProctoc::AppendEntriesArgs> args,
                         std::shared_ptr<raftRpcProctoc::AppendEntriesReply> reply, std::shared_ptr<int> appendNums);

  void pushMsgToKvServer(ApplyMsg msg);
  void readPersist(std::string data);
  std::string persistData();

  void Start(Op command, int* newLogIndex, int* newLogTerm, bool* isLeader);

  void Snapshot(int index, std::string snapshot);

#if ENABLE_READ_INDEX
  bool ReadIndex(int* readIndex);

  bool WaitForApplied(int readIndex, int timeoutMs);
#endif

 public:
  void AppendEntries(google::protobuf::RpcController* controller, const ::raftRpcProctoc::AppendEntriesArgs* request,
                     ::raftRpcProctoc::AppendEntriesReply* response, ::google::protobuf::Closure* done) override;
  void InstallSnapshot(google::protobuf::RpcController* controller,
                       const ::raftRpcProctoc::InstallSnapshotRequest* request,
                       ::raftRpcProctoc::InstallSnapshotResponse* response,
                       ::google::protobuf::Closure* done) override;
  void RequestVote(google::protobuf::RpcController* controller, const ::raftRpcProctoc::RequestVoteArgs* request,
                   ::raftRpcProctoc::RequestVoteReply* response, ::google::protobuf::Closure* done) override;

 public:
  void init(std::vector<std::shared_ptr<RaftRpcUtil>> peers, int me, std::shared_ptr<Persister> persister,
            std::shared_ptr<LockQueue<ApplyMsg>> applyCh);

 private:
  class BoostPersistRaftNode {
   public:
    friend class boost::serialization::access;

    template <class Archive>
    void serialize(Archive& ar, const unsigned int version) {
      ar& m_currentTerm;
      ar& m_votedFor;
      ar& m_lastSnapshotIncludeIndex;
      ar& m_lastSnapshotIncludeTerm;
      ar& m_logs;
    }

    int m_currentTerm;
    int m_votedFor;
    int m_lastSnapshotIncludeIndex;
    int m_lastSnapshotIncludeTerm;
    std::vector<std::string> m_logs;
    std::unordered_map<std::string, int> umap;
  };
};

#endif  // RAFT_H
