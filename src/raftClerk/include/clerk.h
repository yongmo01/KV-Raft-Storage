// Raft KV 客户端封装。

#ifndef SKIP_LIST_ON_RAFT_CLERK_H
#define SKIP_LIST_ON_RAFT_CLERK_H

#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <vector>

#include "config.h"
#include "kvServerRPC.pb.h"
#include "mprpcconfig.h"
#include "raftServerRpcUtil.h"

class Clerk {
 private:
  std::vector<std::shared_ptr<raftServerRpcUtil>> m_servers;
  std::string m_clientId;
  int m_requestId;
  int m_recentLeaderId;

  std::string Uuid() {
    static std::atomic<unsigned long long> nextId{1};
    const auto now = std::chrono::steady_clock::now().time_since_epoch().count();
    const auto seq = nextId.fetch_add(1, std::memory_order_relaxed);
    return std::to_string(now) + "_" + std::to_string(seq);
  }

  void PutAppend(std::string key, std::string value, std::string op);

#if ENABLE_KEY_TTL
  void PutAppendWithTTL(std::string key, std::string value, std::string op, int64_t ttlMs);
#endif

 public:
  Clerk();

  void Init(std::string configFileName);
  std::string Get(std::string key);

  void Put(std::string key, std::string value);
  void Append(std::string key, std::string value);

#if ENABLE_KEY_TTL
  void PutWithTTL(std::string key, std::string value, int64_t ttlMs);
  void AppendWithTTL(std::string key, std::string value, int64_t ttlMs);
#endif
};

#endif  // SKIP_LIST_ON_RAFT_CLERK_H 头文件保护结束
