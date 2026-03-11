#include "kvServer.h"

#include <rpcprovider.h>

#include "mprpcconfig.h"
#include "config.h"
#include "metrics.h"

void KvServer::DprintfKVDB() {
  if (!Debug) {
    return;
  }
  std::lock_guard<std::mutex> lg(m_mtx);
  DEFER {
    // for (const auto &item: m_kvDB) {
    //     DPrintf("[DBInfo ----]Key : %s, Value : %s", &item.first, &item.second);
    // }
    m_skipList.display_list();
  };
}

void KvServer::ExecuteAppendOpOnKVDB(Op op) {
  // if op.IfDuplicate {   //get请求是可重复执行的，因此可以不用判复
  //	return
  // }
  m_mtx.lock();

  m_skipList.insert_set_element(op.Key, op.Value);

  // 【扩展二】TTL: 如果Op携带了TTL，设置key的过期时间
#if ENABLE_KEY_TTL
  if (op.TtlMs > 0) {
    m_expireMap[op.Key] = getCurrentTimeMs() + static_cast<uint64_t>(op.TtlMs);
  }
#endif

  m_lastRequestId[op.ClientId] = op.RequestId;
  m_mtx.unlock();

  DprintfKVDB();
}

void KvServer::ExecuteGetOpOnKVDB(Op op, std::string *value, bool *exist) {
  m_mtx.lock();
  *value = "";
  *exist = false;

  // 【扩展二】TTL: 惰性删除——读取时检查key是否已过期
#if ENABLE_KEY_TTL
  if (isKeyExpired(op.Key)) {
    // key已过期，执行惰性删除
    m_skipList.delete_element(op.Key);
    m_expireMap.erase(op.Key);
    DPrintf("[KvServer::ExecuteGetOpOnKVDB] Key {%s} expired, lazy deleted", op.Key.c_str());
    m_lastRequestId[op.ClientId] = op.RequestId;
    m_mtx.unlock();
    DprintfKVDB();
    return;
  }
#endif

  if (m_skipList.search_element(op.Key, *value)) {
    *exist = true;
  }
  m_lastRequestId[op.ClientId] = op.RequestId;
  m_mtx.unlock();

  DprintfKVDB();
}

void KvServer::ExecutePutOpOnKVDB(Op op) {
  m_mtx.lock();
  m_skipList.insert_set_element(op.Key, op.Value);

  // 【扩展二】TTL: 如果Op携带了TTL，设置key的过期时间
#if ENABLE_KEY_TTL
  if (op.TtlMs > 0) {
    m_expireMap[op.Key] = getCurrentTimeMs() + static_cast<uint64_t>(op.TtlMs);
  } else {
    // TTL为0表示永不过期，如果之前有设置过期时间则清除
    m_expireMap.erase(op.Key);
  }
#endif

  m_lastRequestId[op.ClientId] = op.RequestId;
  m_mtx.unlock();

  DprintfKVDB();
}

// 处理来自clerk的Get RPC
void KvServer::Get(const raftKVRpcProctoc::GetArgs *args, raftKVRpcProctoc::GetReply *reply) {
  // 【扩展四】Metrics: 记录Get请求计数和延迟
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

  // ====================== 【扩展一】ReadIndex 快速读路径 ======================
  // 当ENABLE_READ_INDEX开启时，Get请求不走日志复制流程，
  // 而是通过ReadIndex确认Leadership后直接读取状态机，大幅降低读延迟。
#if ENABLE_READ_INDEX
  int readIndex = -1;
  if (m_raftNode->ReadIndex(&readIndex)) {
    // ReadIndex成功：当前节点是Leader，readIndex是安全的读取点
    // 等待状态机追上readIndex
    if (m_raftNode->WaitForApplied(readIndex, CONSENSUS_TIMEOUT)) {
      std::string value;
      bool exist = false;
      ExecuteGetOpOnKVDB(op, &value, &exist);
      if (exist) {
        reply->set_err(OK);
        reply->set_value(value);
      } else {
        reply->set_err(ErrNoKey);
        reply->set_value("");
      }
      DPrintf("[KvServer::Get-ReadIndex] kvserver{%d} ReadIndex成功, readIndex=%d, key=%s",
              m_me, readIndex, op.Key.c_str());
      return;
    }
    // WaitForApplied超时，降级到原有路径
    DPrintf("[KvServer::Get-ReadIndex] kvserver{%d} WaitForApplied超时, 降级到日志复制路径", m_me);
  }
  // ReadIndex失败（不是Leader），走下面的日志复制路径
#endif
  // ====================== 原有日志复制路径 ======================

  int raftIndex = -1;
  int _ = -1;
  bool isLeader = false;
  m_raftNode->Start(op, &raftIndex, &_,
                    &isLeader);

  if (!isLeader) {
    reply->set_err(ErrWrongLeader);
    return;
  }

  // create waitForCh
  m_mtx.lock();

  if (waitApplyCh.find(raftIndex) == waitApplyCh.end()) {
    waitApplyCh.insert(std::make_pair(raftIndex, new LockQueue<Op>()));
  }
  auto chForRaftIndex = waitApplyCh[raftIndex];

  m_mtx.unlock();  //直接解锁，等待任务执行完成，不能一直拿锁等待

  // timeout
  Op raftCommitOp;

  if (!chForRaftIndex->timeOutPop(CONSENSUS_TIMEOUT, &raftCommitOp)) {
    //        DPrintf("[GET TIMEOUT!!!]From Client %d (Request %d) To Server %d, key %v, raftIndex %d", args.ClientId,
    //        args.RequestId, kv.me, op.Key, raftIndex)
    // todo 2023年06月01日
    int _ = -1;
    bool isLeader = false;
    m_raftNode->GetState(&_, &isLeader);

    if (ifRequestDuplicate(op.ClientId, op.RequestId) && isLeader) {
      //如果超时，代表raft集群不保证已经commitIndex该日志，但是如果是已经提交过的get请求，是可以再执行的。
      // 不会违反线性一致性
      std::string value;
      bool exist = false;
      ExecuteGetOpOnKVDB(op, &value, &exist);
      if (exist) {
        reply->set_err(OK);
        reply->set_value(value);
      } else {
        reply->set_err(ErrNoKey);
        reply->set_value("");
      }
    } else {
      reply->set_err(ErrWrongLeader);  //返回这个，其实就是让clerk换一个节点重试
    }
  } else {
    // raft已经提交了该command（op），可以正式开始执行了
    //         DPrintf("[WaitChanGetRaftApplyMessage<--]Server %d , get Command <-- Index:%d , ClientId %d, RequestId
    //         %d, Opreation %v, Key :%v, Value :%v", kv.me, raftIndex, op.ClientId, op.RequestId, op.Operation, op.Key,
    //         op.Value)
    // todo 这里还要再次检验的原因：感觉不用检验，因为leader只要正确的提交了，那么这些肯定是符合的
    if (raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId) {
      std::string value;
      bool exist = false;
      ExecuteGetOpOnKVDB(op, &value, &exist);
      if (exist) {
        reply->set_err(OK);
        reply->set_value(value);
      } else {
        reply->set_err(ErrNoKey);
        reply->set_value("");
      }
    } else {
      reply->set_err(ErrWrongLeader);
      //            DPrintf("[GET ] 不满足：raftCommitOp.ClientId{%v} == op.ClientId{%v} && raftCommitOp.RequestId{%v}
      //            == op.RequestId{%v}", raftCommitOp.ClientId, op.ClientId, raftCommitOp.RequestId, op.RequestId)
    }
  }
  m_mtx.lock();  // todo 這個可以先弄一個defer，因爲刪除優先級並不高，先把rpc發回去更加重要
  auto tmp = waitApplyCh[raftIndex];
  waitApplyCh.erase(raftIndex);
  delete tmp;
  m_mtx.unlock();
}

void KvServer::GetCommandFromRaft(ApplyMsg message) {
  Op op;
  op.parseFromString(message.Command);

  DPrintf(
      "[KvServer::GetCommandFromRaft-kvserver{%d}] , Got Command --> Index:{%d} , ClientId {%s}, RequestId {%d}, "
      "Opreation {%s}, Key :{%s}, Value :{%s}",
      m_me, message.CommandIndex, &op.ClientId, op.RequestId, &op.Operation, &op.Key, &op.Value);
  if (message.CommandIndex <= m_lastSnapShotRaftLogIndex) {
    return;
  }

  // State Machine (KVServer solute the duplicate problem)
  // duplicate command will not be exed
  if (!ifRequestDuplicate(op.ClientId, op.RequestId)) {
    // execute command
    if (op.Operation == "Put") {
      ExecutePutOpOnKVDB(op);
    }
    if (op.Operation == "Append") {
      ExecuteAppendOpOnKVDB(op);
    }
    //  kv.lastRequestId[op.ClientId] = op.RequestId  在Executexxx函数里面更新的
  }
  //到这里kvDB已经制作了快照
  if (m_maxRaftState != -1) {
    IfNeedToSendSnapShotCommand(message.CommandIndex, 9);
    //如果raft的log太大（大于指定的比例）就把制作快照
  }

  // Send message to the chan of op.ClientId
  SendMessageToWaitChan(op, message.CommandIndex);
}

bool KvServer::ifRequestDuplicate(std::string ClientId, int RequestId) {
  std::lock_guard<std::mutex> lg(m_mtx);
  if (m_lastRequestId.find(ClientId) == m_lastRequestId.end()) {
    return false;
    // todo :不存在这个client就创建
  }
  return RequestId <= m_lastRequestId[ClientId];
}

// get和put//append執行的具體細節是不一樣的
// PutAppend在收到raft消息之後執行，具體函數裏面只判斷冪等性（是否重複）
// get函數收到raft消息之後在，因爲get無論是否重複都可以再執行
void KvServer::PutAppend(const raftKVRpcProctoc::PutAppendArgs *args, raftKVRpcProctoc::PutAppendReply *reply) {
  // 【扩展四】Metrics: 记录PutAppend请求计数和延迟
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

  // 【扩展二】TTL: 从RPC参数中提取TTL信息
#if ENABLE_KEY_TTL
  op.TtlMs = args->ttlms();
#endif

  int raftIndex = -1;
  int _ = -1;
  bool isleader = false;

  m_raftNode->Start(op, &raftIndex, &_, &isleader);

  if (!isleader) {
    DPrintf(
        "[func -KvServer::PutAppend -kvserver{%d}]From Client %s (Request %d) To Server %d, key %s, raftIndex %d , but "
        "not leader",
        m_me, &args->clientid(), args->requestid(), m_me, &op.Key, raftIndex);

    reply->set_err(ErrWrongLeader);
    return;
  }
  DPrintf(
      "[func -KvServer::PutAppend -kvserver{%d}]From Client %s (Request %d) To Server %d, key %s, raftIndex %d , is "
      "leader ",
      m_me, &args->clientid(), args->requestid(), m_me, &op.Key, raftIndex);
  m_mtx.lock();
  if (waitApplyCh.find(raftIndex) == waitApplyCh.end()) {
    waitApplyCh.insert(std::make_pair(raftIndex, new LockQueue<Op>()));
  }
  auto chForRaftIndex = waitApplyCh[raftIndex];

  m_mtx.unlock();  //直接解锁，等待任务执行完成，不能一直拿锁等待

  // timeout
  Op raftCommitOp;

  if (!chForRaftIndex->timeOutPop(CONSENSUS_TIMEOUT, &raftCommitOp)) {
    DPrintf(
        "[func -KvServer::PutAppend -kvserver{%d}]TIMEOUT PUTAPPEND !!!! Server %d , get Command <-- Index:%d , "
        "ClientId %s, RequestId %s, Opreation %s Key :%s, Value :%s",
        m_me, m_me, raftIndex, &op.ClientId, op.RequestId, &op.Operation, &op.Key, &op.Value);

    if (ifRequestDuplicate(op.ClientId, op.RequestId)) {
      reply->set_err(OK);  // 超时了,但因为是重复的请求，返回ok，实际上就算没有超时，在真正执行的时候也要判断是否重复
    } else {
      reply->set_err(ErrWrongLeader);  ///这里返回这个的目的让clerk重新尝试
    }
  } else {
    DPrintf(
        "[func -KvServer::PutAppend -kvserver{%d}]WaitChanGetRaftApplyMessage<--Server %d , get Command <-- Index:%d , "
        "ClientId %s, RequestId %d, Opreation %s, Key :%s, Value :%s",
        m_me, m_me, raftIndex, &op.ClientId, op.RequestId, &op.Operation, &op.Key, &op.Value);
    if (raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId) {
      //可能发生leader的变更导致日志被覆盖，因此必须检查
      reply->set_err(OK);
    } else {
      reply->set_err(ErrWrongLeader);
    }
  }

  m_mtx.lock();

  auto tmp = waitApplyCh[raftIndex];
  waitApplyCh.erase(raftIndex);
  delete tmp;
  m_mtx.unlock();
}

void KvServer::ReadRaftApplyCommandLoop() {
  while (true) {
    //如果只操作applyChan不用拿锁，因为applyChan自己带锁
    auto message = applyChan->Pop();  //阻塞弹出
    DPrintf(
        "---------------tmp-------------[func-KvServer::ReadRaftApplyCommandLoop()-kvserver{%d}] 收到了下raft的消息",
        m_me);
    // listen to every command applied by its raft ,delivery to relative RPC Handler

    if (message.CommandValid) {
      GetCommandFromRaft(message);
    }
    if (message.SnapshotValid) {
      GetSnapShotFromRaft(message);
    }
  }
}

// raft会与persist层交互，kvserver层也会，因为kvserver层开始的时候需要恢复kvdb的状态
//  关于快照raft层与persist的交互：保存kvserver传来的snapshot；生成leaderInstallSnapshot RPC的时候也需要读取snapshot；
//  因此snapshot的具体格式是由kvserver层来定的，raft只负责传递这个东西
//  snapShot里面包含kvserver需要维护的persist_lastRequestId 以及kvDB真正保存的数据persist_kvdb
void KvServer::ReadSnapShotToInstall(std::string snapshot) {
  if (snapshot.empty()) {
    // bootstrap without any state?
    return;
  }
  parseFromString(snapshot);

  //    r := bytes.NewBuffer(snapshot)
  //    d := labgob.NewDecoder(r)
  //
  //    var persist_kvdb map[string]string  //理应快照
  //    var persist_lastRequestId map[int64]int //快照这个为了维护线性一致性
  //
  //    if d.Decode(&persist_kvdb) != nil || d.Decode(&persist_lastRequestId) != nil {
  //                DPrintf("KVSERVER %d read persister got a problem!!!!!!!!!!",kv.me)
  //        } else {
  //        kv.kvDB = persist_kvdb
  //        kv.lastRequestId = persist_lastRequestId
  //    }
}

bool KvServer::SendMessageToWaitChan(const Op &op, int raftIndex) {
  std::lock_guard<std::mutex> lg(m_mtx);
  DPrintf(
      "[RaftApplyMessageSendToWaitChan--> raftserver{%d}] , Send Command --> Index:{%d} , ClientId {%d}, RequestId "
      "{%d}, Opreation {%v}, Key :{%v}, Value :{%v}",
      m_me, raftIndex, &op.ClientId, op.RequestId, &op.Operation, &op.Key, &op.Value);

  if (waitApplyCh.find(raftIndex) == waitApplyCh.end()) {
    return false;
  }
  waitApplyCh[raftIndex]->Push(op);
  DPrintf(
      "[RaftApplyMessageSendToWaitChan--> raftserver{%d}] , Send Command --> Index:{%d} , ClientId {%d}, RequestId "
      "{%d}, Opreation {%v}, Key :{%v}, Value :{%v}",
      m_me, raftIndex, &op.ClientId, op.RequestId, &op.Operation, &op.Key, &op.Value);
  return true;
}

void KvServer::IfNeedToSendSnapShotCommand(int raftIndex, int proportion) {
  if (m_raftNode->GetRaftStateSize() > m_maxRaftState / 10.0) {
    // Send SnapShot Command
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
  std::string snapshotData = getSnapshotData();
  return snapshotData;
}

void KvServer::PutAppend(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::PutAppendArgs *request,
                         ::raftKVRpcProctoc::PutAppendReply *response, ::google::protobuf::Closure *done) {
  KvServer::PutAppend(request, response);
  done->Run();
}

void KvServer::Get(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::GetArgs *request,
                   ::raftKVRpcProctoc::GetReply *response, ::google::protobuf::Closure *done) {
  KvServer::Get(request, response);
  done->Run();
}

KvServer::KvServer(int me, int maxraftstate, std::string nodeInforFileName, short port) : m_skipList(6) {
  std::shared_ptr<Persister> persister = std::make_shared<Persister>(me);

  m_me = me;
  m_maxRaftState = maxraftstate;

  applyChan = std::make_shared<LockQueue<ApplyMsg> >();

  m_raftNode = std::make_shared<Raft>();
  ////////////////clerk层面 kvserver开启rpc接受功能
  //    同时raft与raft节点之间也要开启rpc功能，因此有两个注册
  std::thread t([this, port]() -> void {
    // provider是一个rpc网络服务对象。把UserService对象发布到rpc节点上
    RpcProvider provider;
    provider.NotifyService(this);
    provider.NotifyService(
        this->m_raftNode.get());  // todo：这里获取了原始指针，后面检查一下有没有泄露的问题 或者 shareptr释放的问题
    // 启动一个rpc服务发布节点   Run以后，进程进入阻塞状态，等待远程的rpc调用请求
    provider.Run(m_me, port);
  });
  t.detach();

  ////开启rpc远程调用能力，需要注意必须要保证所有节点都开启rpc接受功能之后才能开启rpc远程调用能力
  ////这里使用睡眠来保证
  std::cout << "raftServer node:" << m_me << " start to sleep to wait all ohter raftnode start!!!!" << std::endl;
  sleep(6);
  std::cout << "raftServer node:" << m_me << " wake up!!!! start to connect other raftnode" << std::endl;
  //获取所有raft节点ip、port ，并进行连接  ,要排除自己
  MprpcConfig config;
  config.LoadConfigFile(nodeInforFileName.c_str());
  std::vector<std::pair<std::string, short> > ipPortVt;
  for (int i = 0; i < INT_MAX - 1; ++i) {
    std::string node = "node" + std::to_string(i);

    std::string nodeIp = config.Load(node + "ip");
    std::string nodePortStr = config.Load(node + "port");
    if (nodeIp.empty()) {
      break;
    }
    ipPortVt.emplace_back(nodeIp, atoi(nodePortStr.c_str()));  //沒有atos方法，可以考慮自己实现
  }
  std::vector<std::shared_ptr<RaftRpcUtil> > servers;
  //进行连接
  for (int i = 0; i < ipPortVt.size(); ++i) {
    if (i == m_me) {
      servers.push_back(nullptr);
      continue;
    }
    std::string otherNodeIp = ipPortVt[i].first;
    short otherNodePort = ipPortVt[i].second;
    auto *rpc = new RaftRpcUtil(otherNodeIp, otherNodePort);
    servers.push_back(std::shared_ptr<RaftRpcUtil>(rpc));

    std::cout << "node" << m_me << " 连接node" << i << "success!" << std::endl;
  }
  sleep(ipPortVt.size() - me);  //等待所有节点相互连接成功，再启动raft
  m_raftNode->init(servers, m_me, persister, applyChan);
  // kv的server直接与raft通信，但kv不直接与raft通信，所以需要把ApplyMsg的chan传递下去用于通信，两者的persist也是共用的

  //////////////////////////////////

  // You may need initialization code here.
  // m_kvDB; //kvdb初始化
  m_skipList;
  waitApplyCh;
  m_lastRequestId;
  m_lastSnapShotRaftLogIndex = 0;  // todo:感覺這個函數沒什麼用，不如直接調用raft節點中的snapshot值？？？
  auto snapshot = persister->ReadSnapshot();
  if (!snapshot.empty()) {
    ReadSnapShotToInstall(snapshot);
  }

  // 【扩展二】TTL: 启动定期清理过期key的后台线程
#if ENABLE_KEY_TTL
  std::thread ttlThread(&KvServer::activeExpireCycle, this);
  ttlThread.detach();
  DPrintf("[KvServer] kvserver{%d} TTL activeExpireCycle thread started", m_me);
#endif

  // 【扩展四】Metrics: 启动定期输出指标的后台线程
#if ENABLE_METRICS
  std::thread metricsThread(&KvServer::metricsDumpLoop, this);
  metricsThread.detach();
  DPrintf("[KvServer] kvserver{%d} Metrics dump thread started", m_me);
#endif

  std::thread t2(&KvServer::ReadRaftApplyCommandLoop, this);  //马上向其他节点宣告自己就是leader
  t2.join();  //由於ReadRaftApplyCommandLoop一直不會結束，达到一直卡在这的目的
}

// ====================== 【扩展二】TTL 过期管理方法实现 ======================
#if ENABLE_KEY_TTL

bool KvServer::isKeyExpired(const std::string &key) {
  // 注意：调用此函数时应持有m_mtx锁
  auto it = m_expireMap.find(key);
  if (it == m_expireMap.end()) {
    return false;  // 没有设置过期时间，永不过期
  }
  return getCurrentTimeMs() > it->second;
}

void KvServer::setKeyExpire(const std::string &key, int64_t ttlMs) {
  // 注意：调用此函数时应持有m_mtx锁
  if (ttlMs > 0) {
    m_expireMap[key] = getCurrentTimeMs() + static_cast<uint64_t>(ttlMs);
  } else {
    m_expireMap.erase(key);  // TTL为0，移除过期时间
  }
}

void KvServer::activeExpireCycle() {
  // 定期删除策略（类似Redis的activeExpireCycle）
  // 每隔一段时间，随机抽样一部分key检查是否过期，过期则删除。
  // 与惰性删除（读时检查）配合，确保过期key能被及时回收。
  while (true) {
    sleepNMilliseconds(TTL_CLEANUP_INTERVAL_MS);
    std::lock_guard<std::mutex> lg(m_mtx);
    int sampled = 0;
    int expired = 0;
    for (auto it = m_expireMap.begin(); it != m_expireMap.end() && sampled < TTL_CLEANUP_SAMPLE_COUNT;) {
      sampled++;
      if (getCurrentTimeMs() > it->second) {
        // key已过期，从skipList和expireMap中删除
        m_skipList.delete_element(it->first);
        DPrintf("[TTL-activeExpireCycle] kvserver{%d} expired key: %s", m_me, it->first.c_str());
        it = m_expireMap.erase(it);
        expired++;
      } else {
        ++it;
      }
    }
    if (expired > 0) {
      DPrintf("[TTL-activeExpireCycle] kvserver{%d} cleaned %d expired keys", m_me, expired);
    }
  }
}
#endif  // ENABLE_KEY_TTL

// ====================== 【扩展四】Metrics 日志输出方法实现 ======================
#if ENABLE_METRICS
void KvServer::metricsDumpLoop() {
  // 定期将Metrics指标输出到日志，便于运行时观测系统状态
  while (true) {
    sleepNMilliseconds(METRICS_DUMP_INTERVAL_MS);
    std::string metricsStr = METRICS_DUMP();
    // 输出到标准输出（和DPrintf保持一致）
    std::cout << "\n[kvserver{" << m_me << "}] " << metricsStr << std::endl;
  }
}
#endif  // ENABLE_METRICS
