// KV 客户端 RPC 调用封装。
#ifndef RAFTSERVERRPC_H
#define RAFTSERVERRPC_H

#include <string>

#include "kvServerRPC.pb.h"
#include "mprpcchannel.h"
#include "mprpccontroller.h"
#include "rpcprovider.h"

class raftServerRpcUtil {
 private:
  raftKVRpcProctoc::kvServerRpc_Stub* stub;

 public:
  raftServerRpcUtil(std::string ip, short port);
  ~raftServerRpcUtil();

  // 主动调用 KVServer 的 Get/PutAppend RPC。
  bool Get(raftKVRpcProctoc::GetArgs* GetArgs, raftKVRpcProctoc::GetReply* reply);
  bool PutAppend(raftKVRpcProctoc::PutAppendArgs* args, raftKVRpcProctoc::PutAppendReply* reply);
};

#endif  // RAFTSERVERRPC_H
