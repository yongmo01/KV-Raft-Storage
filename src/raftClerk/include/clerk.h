//
// Created by swx on 23-6-4.
//

#ifndef SKIP_LIST_ON_RAFT_CLERK_H
#define SKIP_LIST_ON_RAFT_CLERK_H

#include <cstdlib>
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
    return std::to_string(std::rand()) + std::to_string(std::rand()) + std::to_string(std::rand()) +
           std::to_string(std::rand());
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

#endif  // SKIP_LIST_ON_RAFT_CLERK_H
