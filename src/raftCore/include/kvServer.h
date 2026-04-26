#ifndef SKIP_LIST_ON_RAFT_KVSERVER_H
#define SKIP_LIST_ON_RAFT_KVSERVER_H

#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/access.hpp>
#include <boost/serialization/string.hpp>
#include <boost/serialization/unordered_map.hpp>
#include <cstdint>
#include <iostream>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <sstream>
#include <string>
#include <unordered_map>

#include "ApplyMsg.h"
#include "config.h"
#include "kvServerRPC.pb.h"
#include "metrics.h"
#include "raft.h"
#include "kv_engine.h"

class KvServer : public raftKVRpcProctoc::kvServerRpc {
 private:
  // 状态机读写锁：保护 m_engine、m_lastRequestId 和 TTL 元数据。
  // ReadIndex 只读请求使用共享锁；Put/Append/快照恢复使用独占锁。
  mutable std::shared_mutex m_stateMtx;

  // waitApplyCh 只负责 RPC 等待 apply 的通知通道，和 KV 状态机数据拆开加锁。
  std::mutex m_waitApplyMtx;
  int m_me;
  std::shared_ptr<Raft> m_raftNode;
  std::shared_ptr<LockQueue<ApplyMsg>> applyChan;
  int m_maxRaftState;

  std::string m_serializedKVData;
  std::unique_ptr<KVEngine> m_engine;

  std::unordered_map<int, LockQueue<Op>*> waitApplyCh;
  std::unordered_map<std::string, int> m_lastRequestId;

  int m_lastSnapShotRaftLogIndex;

#if ENABLE_KEY_TTL
  // key -> absolute expiration timestamp in milliseconds. Missing keys never expire.
  std::unordered_map<std::string, uint64_t> m_expireMap;
#endif

 public:
  KvServer() = delete;

  KvServer(int me, int maxraftstate, std::string nodeInforFileName, short port);

  void DprintfKVDB();

  void ExecuteAppendOpOnKVDB(Op op);

  void ExecuteGetOpOnKVDB(Op op, std::string* value, bool* exist);

  void ExecuteReadOnlyGetOpOnKVDB(Op op, std::string* value, bool* exist);

  void ExecutePutOpOnKVDB(Op op);

  void Get(const raftKVRpcProctoc::GetArgs* args, raftKVRpcProctoc::GetReply* reply);

  void GetCommandFromRaft(ApplyMsg message);

  bool ifRequestDuplicate(std::string ClientId, int RequestId);

  void PutAppend(const raftKVRpcProctoc::PutAppendArgs* args, raftKVRpcProctoc::PutAppendReply* reply);

  void ReadRaftApplyCommandLoop();

  void ReadSnapShotToInstall(std::string snapshot);

  bool SendMessageToWaitChan(const Op& op, int raftIndex);

  void IfNeedToSendSnapShotCommand(int raftIndex, int proportion);

  void GetSnapShotFromRaft(ApplyMsg message);

  std::string MakeSnapShot();

#if ENABLE_KEY_TTL
  bool isKeyExpired(const std::string& key);

  void setKeyExpire(const std::string& key, int64_t expireAtMs);

  void activeExpireCycle();
#endif

#if ENABLE_METRICS
  void metricsDumpLoop();
#endif

 public:
  void PutAppend(google::protobuf::RpcController* controller, const ::raftKVRpcProctoc::PutAppendArgs* request,
                 ::raftKVRpcProctoc::PutAppendReply* response, ::google::protobuf::Closure* done) override;

  void Get(google::protobuf::RpcController* controller, const ::raftKVRpcProctoc::GetArgs* request,
           ::raftKVRpcProctoc::GetReply* response, ::google::protobuf::Closure* done) override;

 private:
  friend class boost::serialization::access;

  template <class Archive>
  void serialize(Archive& ar, const unsigned int version) {
    ar& m_serializedKVData;
    ar& m_lastRequestId;
#if ENABLE_KEY_TTL
    ar& m_expireMap;
#endif
  }

  std::string getSnapshotData() {
    // 调用方必须持有 m_stateMtx 的独占锁，避免快照过程中状态机被写入或恢复。
    m_serializedKVData = m_engine->Snapshot();
    std::stringstream ss;
    boost::archive::text_oarchive oa(ss);
    oa << *this;
    m_serializedKVData.clear();
    return ss.str();
  }

  void parseFromString(const std::string& str) {
    // 调用方必须持有 m_stateMtx 的独占锁，避免恢复快照时和读写请求并发访问状态机。
    std::stringstream ss(str);
    boost::archive::text_iarchive ia(ss);
    ia >> *this;
    m_engine->Restore(m_serializedKVData);
    m_serializedKVData.clear();
  }
};

#endif  // SKIP_LIST_ON_RAFT_KVSERVER_H
