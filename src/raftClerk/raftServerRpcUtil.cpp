// KV 客户端到 KVServer 的 RPC 调用封装。
#include "raftServerRpcUtil.h"

#include "util.h"

raftServerRpcUtil::raftServerRpcUtil(std::string ip, short port) {
  stub = new raftKVRpcProctoc::kvServerRpc_Stub(new MprpcChannel(ip, port, false));
}

raftServerRpcUtil::~raftServerRpcUtil() { delete stub; }

bool raftServerRpcUtil::Get(raftKVRpcProctoc::GetArgs* GetArgs, raftKVRpcProctoc::GetReply* reply) {
  MprpcController controller;
  stub->Get(&controller, GetArgs, reply, nullptr);
  if (controller.Failed()) {
    DPrintf("[raftServerRpcUtil::Get] rpc failed: %s", controller.ErrorText().c_str());
  }
  return !controller.Failed();
}

bool raftServerRpcUtil::PutAppend(raftKVRpcProctoc::PutAppendArgs* args, raftKVRpcProctoc::PutAppendReply* reply) {
  MprpcController controller;
  stub->PutAppend(&controller, args, reply, nullptr);
  if (controller.Failed()) {
    DPrintf("[raftServerRpcUtil::PutAppend] rpc failed: %s", controller.ErrorText().c_str());
  }
  return !controller.Failed();
}
