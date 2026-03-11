//
// Created by swx on 23-6-4.
//

#ifndef SKIP_LIST_ON_RAFT_CLERK_H
#define SKIP_LIST_ON_RAFT_CLERK_H
#include <arpa/inet.h>
#include <netinet/in.h>
#include <raftServerRpcUtil.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <cerrno>
#include <string>
#include <vector>
#include "kvServerRPC.pb.h"
#include "mprpcconfig.h"
#include "config.h"

class Clerk {
 private:
  std::vector<std::shared_ptr<raftServerRpcUtil>>
      m_servers;  //保存所有raft节点的fd //todo：全部初始化为-1，表示没有连接上
  std::string m_clientId;
  int m_requestId;
  int m_recentLeaderId;  //只是有可能是领导

  std::string Uuid() {
    return std::to_string(rand()) + std::to_string(rand()) + std::to_string(rand()) + std::to_string(rand());
  }  //用于返回随机的clientId

  //    MakeClerk  todo
  void PutAppend(std::string key, std::string value, std::string op);

  // 【扩展二】支持TTL的PutAppend内部方法
#if ENABLE_KEY_TTL
  void PutAppendWithTTL(std::string key, std::string value, std::string op, int64_t ttlMs);
#endif

 public:
  //对外暴露的三个功能和初始化
  void Init(std::string configFileName);
  std::string Get(std::string key);

  void Put(std::string key, std::string value);
  void Append(std::string key, std::string value);

  // 【扩展二】支持TTL的Put和Append接口，ttlMs为过期时间（毫秒），0表示永不过期
#if ENABLE_KEY_TTL
  void PutWithTTL(std::string key, std::string value, int64_t ttlMs);
  void AppendWithTTL(std::string key, std::string value, int64_t ttlMs);
#endif

 public:
  Clerk();
};

#endif  // SKIP_LIST_ON_RAFT_CLERK_H
