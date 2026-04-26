#include "kvServer.h"

#include <climits>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <thread>
#include <utility>
#include <vector>

#include <rpcprovider.h>
#include <unistd.h>

#include "cached_kv_engine.h"
#include "config.h"
#include "lsm_tree_engine.h"
#include "metrics.h"
#include "mprpcconfig.h"
#include "skiplist_engine.h"

namespace {

std::unique_ptr<KVEngine> CreateDefaultKVEngine() {
#if ENABLE_LSM_TREE
  auto base = std::make_unique<LSMTreeEngine>(LSM_MEMTABLE_FLUSH_THRESHOLD);
#else
  auto base = std::make_unique<SkipListEngine>(6);
#endif
#if ENABLE_LRU_CACHE
  return std::make_unique<CachedKVEngine>(std::move(base), LRU_CACHE_CAPACITY);
#else
  return std::unique_ptr<KVEngine>(std::move(base));
#endif
}

void FillGetReply(raftKVRpcProctoc::GetReply* reply, bool exist, const std::string& value) {
  if (exist) {
    reply->set_err(OK);
    reply->set_value(value);
    return;
  }
  reply->set_err(ErrNoKey);
  reply->set_value("");
}

}  // namespace

void KvServer::DprintfKVDB() {
  if (!Debug) {
    return;
  }
  std::lock_guard<std::mutex> lg(m_mtx);
  if (m_engine != nullptr) {
    m_engine->Display();
  }
}

void KvServer::ExecuteAppendOpOnKVDB(Op op) {
  std::lock_guard<std::mutex> lg(m_mtx);

#if ENABLE_KEY_TTL
  if (isKeyExpired(op.Key)) {
    m_engine->Delete(op.Key);
    m_expireMap.erase(op.Key);
  }
#endif

  std::string oldValue;
  std::string newValue = op.Value;
  if (m_engine->Get(op.Key, &oldValue)) {
    newValue = oldValue + op.Value;
  }
  m_engine->Put(op.Key, newValue);

#if ENABLE_KEY_TTL
  setKeyExpire(op.Key, op.ExpireAtMs);
#endif

  m_lastRequestId[op.ClientId] = op.RequestId;
}

void KvServer::ExecuteGetOpOnKVDB(Op op, std::string* value, bool* exist) {
  std::lock_guard<std::mutex> lg(m_mtx);
  *value = "";
  *exist = false;

#if ENABLE_KEY_TTL
  if (isKeyExpired(op.Key)) {
    m_engine->Delete(op.Key);
    m_expireMap.erase(op.Key);
    DPrintf("[KvServer::ExecuteGetOpOnKVDB] key {%s} expired and was deleted", op.Key.c_str());
    m_lastRequestId[op.ClientId] = op.RequestId;
    return;
  }
#endif

  if (m_engine->Get(op.Key, value)) {
    *exist = true;
  }
  m_lastRequestId[op.ClientId] = op.RequestId;
}

void KvServer::ExecutePutOpOnKVDB(Op op) {
  std::lock_guard<std::mutex> lg(m_mtx);
  m_engine->Put(op.Key, op.Value);

#if ENABLE_KEY_TTL
  setKeyExpire(op.Key, op.ExpireAtMs);
#endif

  m_lastRequestId[op.ClientId] = op.RequestId;
}

void KvServer::Get(const raftKVRpcProctoc::GetArgs* args, raftKVRpcProctoc::GetReply* reply) {
#if ENABLE_METRICS
  METRICS_INC_COUNTER("kv_get_total");
  auto metricsStart = std::chrono::high_resolution_clock::now();
  DEFER {
    auto metricsEnd = std::chrono::high_resolution_clock::now();
    auto us = std::chrono::duration_cast<std::chrono::microseconds>(metricsEnd - metricsStart).count();
    METRICS_RECORD_LATENCY("kv_get_latency_us", us);
  };
#endif

  Op op;
  op.Operation = "Get";
  op.Key = args->key();
  op.Value = "";
  op.ClientId = args->clientid();
  op.RequestId = args->requestid();

#if ENABLE_READ_INDEX
  int readIndex = -1;
  if (m_raftNode->ReadIndex(&readIndex)) {
    if (m_raftNode->WaitForApplied(readIndex, CONSENSUS_TIMEOUT)) {
      std::string value;
      bool exist = false;
      ExecuteGetOpOnKVDB(op, &value, &exist);
      FillGetReply(reply, exist, value);
      DPrintf("[KvServer::Get-ReadIndex] server{%d} readIndex=%d key=%s", m_me, readIndex, op.Key.c_str());
      return;
    }
    DPrintf("[KvServer::Get-ReadIndex] server{%d} timed out waiting for readIndex=%d", m_me, readIndex);
  }
#endif

  int raftIndex = -1;
  int term = -1;
  bool isLeader = false;
  m_raftNode->Start(op, &raftIndex, &term, &isLeader);
  if (!isLeader) {
    reply->set_err(ErrWrongLeader);
    return;
  }

  LockQueue<Op>* chForRaftIndex = nullptr;
  {
    std::lock_guard<std::mutex> lg(m_mtx);
    if (waitApplyCh.find(raftIndex) == waitApplyCh.end()) {
      waitApplyCh.insert(std::make_pair(raftIndex, new LockQueue<Op>()));
    }
    chForRaftIndex = waitApplyCh[raftIndex];
  }

  Op raftCommitOp;
  if (!chForRaftIndex->timeOutPop(CONSENSUS_TIMEOUT, &raftCommitOp)) {
    int currentTerm = -1;
    bool stillLeader = false;
    m_raftNode->GetState(&currentTerm, &stillLeader);

    if (ifRequestDuplicate(op.ClientId, op.RequestId) && stillLeader) {
      std::string value;
      bool exist = false;
      ExecuteGetOpOnKVDB(op, &value, &exist);
      FillGetReply(reply, exist, value);
    } else {
      reply->set_err(ErrWrongLeader);
    }
  } else if (raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId) {
    std::string value;
    bool exist = false;
    ExecuteGetOpOnKVDB(op, &value, &exist);
    FillGetReply(reply, exist, value);
  } else {
    reply->set_err(ErrWrongLeader);
  }

  {
    std::lock_guard<std::mutex> lg(m_mtx);
    auto it = waitApplyCh.find(raftIndex);
    if (it != waitApplyCh.end()) {
      delete it->second;
      waitApplyCh.erase(it);
    }
  }
}

void KvServer::GetCommandFromRaft(ApplyMsg message) {
  Op op;
  op.parseFromString(message.Command);

  DPrintf("[KvServer::GetCommandFromRaft] server{%d} index=%d client=%s request=%d op=%s key=%s",
          m_me, message.CommandIndex, op.ClientId.c_str(), op.RequestId, op.Operation.c_str(), op.Key.c_str());

  if (message.CommandIndex <= m_lastSnapShotRaftLogIndex) {
    return;
  }

  if (!ifRequestDuplicate(op.ClientId, op.RequestId)) {
    if (op.Operation == "Put") {
      ExecutePutOpOnKVDB(op);
    } else if (op.Operation == "Append") {
      ExecuteAppendOpOnKVDB(op);
    } else if (op.Operation == "Noop") {
      // Noop 只用于 Raft 新 leader 推进 commitIndex，不修改 KV 状态机。
    }
  }

  if (m_maxRaftState != -1) {
    IfNeedToSendSnapShotCommand(message.CommandIndex, 9);
  }

  SendMessageToWaitChan(op, message.CommandIndex);
}

bool KvServer::ifRequestDuplicate(std::string ClientId, int RequestId) {
  std::lock_guard<std::mutex> lg(m_mtx);
  auto it = m_lastRequestId.find(ClientId);
  if (it == m_lastRequestId.end()) {
    return false;
  }
  return RequestId <= it->second;
}

void KvServer::PutAppend(const raftKVRpcProctoc::PutAppendArgs* args, raftKVRpcProctoc::PutAppendReply* reply) {
#if ENABLE_METRICS
  METRICS_INC_COUNTER("kv_putappend_total");
  auto metricsStart = std::chrono::high_resolution_clock::now();
  DEFER {
    auto metricsEnd = std::chrono::high_resolution_clock::now();
    auto us = std::chrono::duration_cast<std::chrono::microseconds>(metricsEnd - metricsStart).count();
    METRICS_RECORD_LATENCY("kv_putappend_latency_us", us);
  };
#endif

  Op op;
  op.Operation = args->op();
  op.Key = args->key();
  op.Value = args->value();
  op.ClientId = args->clientid();
  op.RequestId = args->requestid();

#if ENABLE_KEY_TTL
  op.TtlMs = args->ttlms();
  op.ExpireAtMs = op.TtlMs > 0 ? getCurrentTimeMs() + static_cast<uint64_t>(op.TtlMs) : 0;
#endif

  int raftIndex = -1;
  int term = -1;
  bool isLeader = false;
  m_raftNode->Start(op, &raftIndex, &term, &isLeader);

  if (!isLeader) {
    DPrintf("[KvServer::PutAppend] server{%d} is not leader, client=%s request=%d key=%s",
            m_me, args->clientid().c_str(), args->requestid(), op.Key.c_str());
    reply->set_err(ErrWrongLeader);
    return;
  }

  LockQueue<Op>* chForRaftIndex = nullptr;
  {
    std::lock_guard<std::mutex> lg(m_mtx);
    if (waitApplyCh.find(raftIndex) == waitApplyCh.end()) {
      waitApplyCh.insert(std::make_pair(raftIndex, new LockQueue<Op>()));
    }
    chForRaftIndex = waitApplyCh[raftIndex];
  }

  Op raftCommitOp;
  if (!chForRaftIndex->timeOutPop(CONSENSUS_TIMEOUT, &raftCommitOp)) {
    if (ifRequestDuplicate(op.ClientId, op.RequestId)) {
      reply->set_err(OK);
    } else {
      reply->set_err(ErrWrongLeader);
    }
  } else if (raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId) {
    reply->set_err(OK);
  } else {
    reply->set_err(ErrWrongLeader);
  }

  {
    std::lock_guard<std::mutex> lg(m_mtx);
    auto it = waitApplyCh.find(raftIndex);
    if (it != waitApplyCh.end()) {
      delete it->second;
      waitApplyCh.erase(it);
    }
  }
}

void KvServer::ReadRaftApplyCommandLoop() {
  while (true) {
    auto message = applyChan->Pop();
    DPrintf("[KvServer::ReadRaftApplyCommandLoop] server{%d} received apply message", m_me);

    if (message.CommandValid) {
      GetCommandFromRaft(message);
    }
    if (message.SnapshotValid) {
      GetSnapShotFromRaft(message);
    }
  }
}

void KvServer::ReadSnapShotToInstall(std::string snapshot) {
  if (snapshot.empty()) {
    return;
  }
  parseFromString(snapshot);
}

bool KvServer::SendMessageToWaitChan(const Op& op, int raftIndex) {
  std::lock_guard<std::mutex> lg(m_mtx);
  auto it = waitApplyCh.find(raftIndex);
  if (it == waitApplyCh.end()) {
    return false;
  }

  it->second->Push(op);
  DPrintf("[KvServer::SendMessageToWaitChan] server{%d} index=%d client=%s request=%d op=%s key=%s",
          m_me, raftIndex, op.ClientId.c_str(), op.RequestId, op.Operation.c_str(), op.Key.c_str());
  return true;
}

void KvServer::IfNeedToSendSnapShotCommand(int raftIndex, int proportion) {
  if (m_maxRaftState <= 0) {
    return;
  }

  if (m_raftNode->GetRaftStateSize() > m_maxRaftState * proportion / 10.0) {
    auto snapshot = MakeSnapShot();
    m_raftNode->Snapshot(raftIndex, snapshot);
  }
}

void KvServer::GetSnapShotFromRaft(ApplyMsg message) {
  std::lock_guard<std::mutex> lg(m_mtx);

  if (m_raftNode->CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot)) {
    ReadSnapShotToInstall(message.Snapshot);
    m_lastSnapShotRaftLogIndex = message.SnapshotIndex;
  }
}

std::string KvServer::MakeSnapShot() {
  std::lock_guard<std::mutex> lg(m_mtx);
  return getSnapshotData();
}

void KvServer::PutAppend(google::protobuf::RpcController* controller,
                         const ::raftKVRpcProctoc::PutAppendArgs* request,
                         ::raftKVRpcProctoc::PutAppendReply* response,
                         ::google::protobuf::Closure* done) {
  KvServer::PutAppend(request, response);
  done->Run();
}

void KvServer::Get(google::protobuf::RpcController* controller,
                   const ::raftKVRpcProctoc::GetArgs* request,
                   ::raftKVRpcProctoc::GetReply* response,
                   ::google::protobuf::Closure* done) {
  KvServer::Get(request, response);
  done->Run();
}

KvServer::KvServer(int me, int maxraftstate, std::string nodeInforFileName, short port) {
  std::shared_ptr<Persister> persister = std::make_shared<Persister>(me);

  m_me = me;
  m_maxRaftState = maxraftstate;
  m_engine = CreateDefaultKVEngine();
  applyChan = std::make_shared<LockQueue<ApplyMsg>>();
  m_raftNode = std::make_shared<Raft>();

  std::thread rpcThread([this, port, nodeInforFileName]() -> void {
    RpcProvider provider;
    provider.NotifyService(this);
    provider.NotifyService(this->m_raftNode.get());
    provider.Run(m_me, port, nodeInforFileName);
  });
  rpcThread.detach();

  std::cout << "raftServer node:" << m_me << " waiting for peer startup" << std::endl;
  sleep(6);
  std::cout << "raftServer node:" << m_me << " connecting peers" << std::endl;

  MprpcConfig config;
  config.LoadConfigFile(nodeInforFileName.c_str());
  std::vector<std::pair<std::string, short>> ipPortVt;
  for (int i = 0; i < INT_MAX - 1; ++i) {
    std::string node = "node" + std::to_string(i);
    std::string nodeIp = config.Load(node + "ip");
    std::string nodePortStr = config.Load(node + "port");
    if (nodeIp.empty()) {
      break;
    }
    ipPortVt.emplace_back(nodeIp, static_cast<short>(std::atoi(nodePortStr.c_str())));
  }

  std::vector<std::shared_ptr<RaftRpcUtil>> servers;
  for (int i = 0; i < ipPortVt.size(); ++i) {
    if (i == m_me) {
      servers.push_back(nullptr);
      continue;
    }

    auto* rpc = new RaftRpcUtil(ipPortVt[i].first, ipPortVt[i].second);
    servers.push_back(std::shared_ptr<RaftRpcUtil>(rpc));
    std::cout << "node" << m_me << " connected node" << i << " successfully" << std::endl;
  }

  int startupDelaySeconds = static_cast<int>(ipPortVt.size()) - me;
  if (startupDelaySeconds > 0) {
    sleep(static_cast<unsigned int>(startupDelaySeconds));
  }
  m_raftNode->init(servers, m_me, persister, applyChan);

  m_lastSnapShotRaftLogIndex = 0;
  auto snapshot = persister->ReadSnapshot();
  if (!snapshot.empty()) {
    ReadSnapShotToInstall(snapshot);
  }

#if ENABLE_KEY_TTL && ENABLE_TTL_ACTIVE_EXPIRE
  std::thread ttlThread(&KvServer::activeExpireCycle, this);
  ttlThread.detach();
  DPrintf("[KvServer] server{%d} TTL active-expire observer thread started", m_me);
#endif

#if ENABLE_METRICS
  std::thread metricsThread(&KvServer::metricsDumpLoop, this);
  metricsThread.detach();
  DPrintf("[KvServer] server{%d} metrics thread started", m_me);
#endif

  std::thread applyThread(&KvServer::ReadRaftApplyCommandLoop, this);
  applyThread.join();
}

#if ENABLE_KEY_TTL

bool KvServer::isKeyExpired(const std::string& key) {
  auto it = m_expireMap.find(key);
  if (it == m_expireMap.end()) {
    return false;
  }
  return getCurrentTimeMs() > it->second;
}

void KvServer::setKeyExpire(const std::string& key, int64_t expireAtMs) {
  if (expireAtMs > 0) {
    m_expireMap[key] = static_cast<uint64_t>(expireAtMs);
  } else {
    m_expireMap.erase(key);
  }
}

void KvServer::activeExpireCycle() {
  while (true) {
    sleepNMilliseconds(TTL_CLEANUP_INTERVAL_MS);
    std::lock_guard<std::mutex> lg(m_mtx);

    int sampled = 0;
    int expired = 0;
    for (auto it = m_expireMap.begin(); it != m_expireMap.end() && sampled < TTL_CLEANUP_SAMPLE_COUNT; ++it) {
      ++sampled;
      if (getCurrentTimeMs() > it->second) {
        ++expired;
      }
    }

    if (expired > 0) {
      DPrintf("[TTL-activeExpireCycle] server{%d} observed %d expired keys; logical cleanup is lazy-on-read", m_me,
              expired);
    }
  }
}
#endif  // ENABLE_KEY_TTL

#if ENABLE_METRICS
void KvServer::metricsDumpLoop() {
  while (true) {
    sleepNMilliseconds(METRICS_DUMP_INTERVAL_MS);
    std::cout << "\n[kvserver{" << m_me << "}] " << METRICS_DUMP() << std::endl;
  }
}
#endif  // ENABLE_METRICS
