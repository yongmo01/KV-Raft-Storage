//
// Created by swx on 23-6-1.
//

#ifndef SKIP_LIST_ON_RAFT_KVSERVER_H
#define SKIP_LIST_ON_RAFT_KVSERVER_H

#include <boost/any.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/foreach.hpp>
#include <boost/serialization/export.hpp>
#include <boost/serialization/serialization.hpp>
#include <boost/serialization/unordered_map.hpp>
#include <boost/serialization/vector.hpp>
#include <iostream>
#include <mutex>
#include <unordered_map>
#include "kvServerRPC.pb.h"
#include "raft.h"
#include "skipList.h"
#include "config.h"
#include "metrics.h"

class KvServer : raftKVRpcProctoc::kvServerRpc {
 private:
  std::mutex m_mtx;
  int m_me;
  std::shared_ptr<Raft> m_raftNode;
  std::shared_ptr<LockQueue<ApplyMsg> > applyChan;  // kvServer和raft节点的通信管道
  int m_maxRaftState;                               // snapshot if log grows this big

  // Your definitions here.
  std::string m_serializedKVData;  // todo ： 序列化后的kv数据，理论上可以不用，但是目前没有找到特别好的替代方法
  SkipList<std::string, std::string> m_skipList;
  std::unordered_map<std::string, std::string> m_kvDB;

  std::unordered_map<int, LockQueue<Op> *> waitApplyCh;
  // index(raft) -> chan  //？？？字段含义   waitApplyCh是一个map，键是int，值是Op类型的管道

  std::unordered_map<std::string, int> m_lastRequestId;  // clientid -> requestID  //一个kV服务器可能连接多个client

  // last SnapShot point , raftIndex
  int m_lastSnapShotRaftLogIndex;

  // ====================== 【扩展二】TTL 过期管理 ======================
#if ENABLE_KEY_TTL
  // key -> 过期时间戳(绝对时间，毫秒)。如果key不在此map中，则表示永不过期。
  std::unordered_map<std::string, uint64_t> m_expireMap;
#endif

 public:
  KvServer() = delete;

  KvServer(int me, int maxraftstate, std::string nodeInforFileName, short port);

  void StartKVServer();

  void DprintfKVDB();

  void ExecuteAppendOpOnKVDB(Op op);

  void ExecuteGetOpOnKVDB(Op op, std::string *value, bool *exist);

  void ExecutePutOpOnKVDB(Op op);

  void Get(const raftKVRpcProctoc::GetArgs *args,
           raftKVRpcProctoc::GetReply
               *reply);  //将 GetArgs 改为rpc调用的，因为是远程客户端，即服务器宕机对客户端来说是无感的
  /**
   * 從raft節點中獲取消息  （不要誤以爲是執行【GET】命令）
   * @param message
   */
  void GetCommandFromRaft(ApplyMsg message);

  bool ifRequestDuplicate(std::string ClientId, int RequestId);

  // clerk 使用RPC远程调用
  void PutAppend(const raftKVRpcProctoc::PutAppendArgs *args, raftKVRpcProctoc::PutAppendReply *reply);

  ////一直等待raft传来的applyCh
  void ReadRaftApplyCommandLoop();

  void ReadSnapShotToInstall(std::string snapshot);

  bool SendMessageToWaitChan(const Op &op, int raftIndex);

  // 检查是否需要制作快照，需要的话就向raft之下制作快照
  void IfNeedToSendSnapShotCommand(int raftIndex, int proportion);

  // Handler the SnapShot from kv.rf.applyCh
  void GetSnapShotFromRaft(ApplyMsg message);

  std::string MakeSnapShot();

  // ====================== 【扩展二】TTL 管理方法 ======================
#if ENABLE_KEY_TTL
  /**
   * @brief 检查指定key是否已过期（惰性删除策略的核心）
   * @param key 要检查的key
   * @return true表示已过期，false表示未过期或无TTL设置
   */
  bool isKeyExpired(const std::string &key);

  /**
   * @brief 设置key的过期时间
   * @param key 要设置的key
   * @param ttlMs 过期时间（毫秒），0表示永不过期
   */
  void setKeyExpire(const std::string &key, int64_t ttlMs);

  /**
   * @brief 定期清理过期key的后台线程函数（定期删除策略）
   * 每隔TTL_CLEANUP_INTERVAL_MS毫秒，随机抽样TTL_CLEANUP_SAMPLE_COUNT个key进行检查
   */
  void activeExpireCycle();
#endif

  // ====================== 【扩展四】Metrics 日志输出 ======================
#if ENABLE_METRICS
  /**
   * @brief 定期输出Metrics指标到日志的后台线程函数
   */
  void metricsDumpLoop();
#endif

 public:  // for rpc
  void PutAppend(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::PutAppendArgs *request,
                 ::raftKVRpcProctoc::PutAppendReply *response, ::google::protobuf::Closure *done) override;

  void Get(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::GetArgs *request,
           ::raftKVRpcProctoc::GetReply *response, ::google::protobuf::Closure *done) override;

  /////////////////serialiazation start ///////////////////////////////
  // notice ： func serialize
 private:
  friend class boost::serialization::access;

  // When the class Archive corresponds to an output archive, the
  // & operator is defined similar to <<.  Likewise, when the class Archive
  // is a type of input archive the & operator is defined similar to >>.
  template <class Archive>
  void serialize(Archive &ar, const unsigned int version)  //这里面写需要序列话和反序列化的字段
  {
    ar &m_serializedKVData;

    // ar & m_kvDB;
    ar &m_lastRequestId;
  }

  std::string getSnapshotData() {
    m_serializedKVData = m_skipList.dump_file();
    std::stringstream ss;
    boost::archive::text_oarchive oa(ss);
    oa << *this;
    m_serializedKVData.clear();
    return ss.str();
  }

  void parseFromString(const std::string &str) {
    std::stringstream ss(str);
    boost::archive::text_iarchive ia(ss);
    ia >> *this;
    m_skipList.load_file(m_serializedKVData);
    m_serializedKVData.clear();
  }

  /////////////////serialiazation end ///////////////////////////////
};

#endif  // SKIP_LIST_ON_RAFT_KVSERVER_H
