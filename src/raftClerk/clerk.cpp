//
// Created by swx on 23-6-4.
//

#include "clerk.h"

#include <climits>
#include <cstdlib>
#include <string>
#include <utility>
#include <vector>

#include "raftServerRpcUtil.h"
#include "util.h"

std::string Clerk::Get(std::string key) {
  myAssert(!m_servers.empty(), "Clerk has no raft servers. Call Init before sending requests.");
  ++m_requestId;
  auto requestId = m_requestId;
  int server = m_recentLeaderId;
  int serverCount = static_cast<int>(m_servers.size());

  raftKVRpcProctoc::GetArgs args;
  args.set_key(key);
  args.set_clientid(m_clientId);
  args.set_requestid(requestId);

  while (true) {
    raftKVRpcProctoc::GetReply reply;
    bool ok = m_servers[server]->Get(&args, &reply);
    if (!ok || reply.err() == ErrWrongLeader) {
      server = (server + 1) % serverCount;
      continue;
    }
    if (reply.err() == ErrNoKey) {
      return "";
    }
    if (reply.err() == OK) {
      m_recentLeaderId = server;
      return reply.value();
    }
  }
  return "";
}

void Clerk::PutAppend(std::string key, std::string value, std::string op) {
  myAssert(!m_servers.empty(), "Clerk has no raft servers. Call Init before sending requests.");
  ++m_requestId;
  auto requestId = m_requestId;
  auto server = m_recentLeaderId;
  int serverCount = static_cast<int>(m_servers.size());

  while (true) {
    raftKVRpcProctoc::PutAppendArgs args;
    args.set_key(key);
    args.set_value(value);
    args.set_op(op);
    args.set_clientid(m_clientId);
    args.set_requestid(requestId);

    raftKVRpcProctoc::PutAppendReply reply;
    bool ok = m_servers[server]->PutAppend(&args, &reply);
    if (!ok || reply.err() == ErrWrongLeader) {
      DPrintf("[Clerk::PutAppend] request failed on server{%d}, retry server{%d}, op=%s", server,
              (server + 1) % serverCount, op.c_str());
      server = (server + 1) % serverCount;
      continue;
    }
    if (reply.err() == OK) {
      m_recentLeaderId = server;
      return;
    }
  }
}

void Clerk::Put(std::string key, std::string value) { PutAppend(key, value, "Put"); }

void Clerk::Append(std::string key, std::string value) { PutAppend(key, value, "Append"); }

#if ENABLE_KEY_TTL
void Clerk::PutAppendWithTTL(std::string key, std::string value, std::string op, int64_t ttlMs) {
  myAssert(!m_servers.empty(), "Clerk has no raft servers. Call Init before sending requests.");
  ++m_requestId;
  auto requestId = m_requestId;
  auto server = m_recentLeaderId;
  int serverCount = static_cast<int>(m_servers.size());

  while (true) {
    raftKVRpcProctoc::PutAppendArgs args;
    args.set_key(key);
    args.set_value(value);
    args.set_op(op);
    args.set_clientid(m_clientId);
    args.set_requestid(requestId);
    args.set_ttlms(ttlMs);

    raftKVRpcProctoc::PutAppendReply reply;
    bool ok = m_servers[server]->PutAppend(&args, &reply);
    if (!ok || reply.err() == ErrWrongLeader) {
      server = (server + 1) % serverCount;
      continue;
    }
    if (reply.err() == OK) {
      m_recentLeaderId = server;
      return;
    }
  }
}

void Clerk::PutWithTTL(std::string key, std::string value, int64_t ttlMs) {
  PutAppendWithTTL(key, value, "Put", ttlMs);
}

void Clerk::AppendWithTTL(std::string key, std::string value, int64_t ttlMs) {
  PutAppendWithTTL(key, value, "Append", ttlMs);
}
#endif  // ENABLE_KEY_TTL

void Clerk::Init(std::string configFileName) {
  MprpcConfig config;
  config.LoadConfigFile(configFileName.c_str());

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

  for (const auto& item : ipPortVt) {
    auto* rpc = new raftServerRpcUtil(item.first, item.second);
    m_servers.push_back(std::shared_ptr<raftServerRpcUtil>(rpc));
  }
}

Clerk::Clerk() : m_clientId(Uuid()), m_requestId(0), m_recentLeaderId(0) {}
