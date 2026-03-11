#ifndef RAFT_H
#define RAFT_H

#include <boost/serialization/string.hpp>      // boost序列化支持string类型
#include <boost/serialization/vector.hpp>      // boost序列化支持vector类型
#include <chrono>                             // 时间相关，如超时、定时器
#include <cmath>                              // 数学函数，如std::abs、std::pow等
#include <iostream>                           // 标准输入输出流，调试打印等
#include <memory>                             // 智能指针，如std::shared_ptr、std::unique_ptr
#include <mutex>                              // 互斥锁std::mutex，多线程同步
#include <string>                             // 字符串std::string
#include <thread>                             // 多线程std::thread
#include <vector>                             // 动态数组std::vector
#include "ApplyMsg.h"                        // 日志应用消息结构体定义
#include "Persister.h"                       // 持久化相关接口和实现
#include "boost/any.hpp"                     // boost任意类型容器any
#include "boost/serialization/serialization.hpp" // boost序列化主头文件
#include "config.h"                          // 配置相关定义
#include "monsoon.h"                         // 协程/IO管理器相关定义
#include "raftRpcUtil.h"                     // raft RPC工具类
#include "util.h"                            // 工具函数、通用辅助方法
#include "metrics.h"                         // 【扩展四】运行时监控指标
/// @brief //////////// 网络状态表示  todo：可以在rpc中删除该字段，实际生产中是用不到的.
constexpr int Disconnected =
    0;  // 方便网络分区的时候debug，网络异常的时候为disconnected，只要网络正常就为AppNormal，防止matchIndex[]数组异常减小
constexpr int AppNormal = 1;

///////////////投票状态

constexpr int Killed = 0;
constexpr int Voted = 1;   //本轮已经投过票了
constexpr int Expire = 2;  //投票（消息、竞选者）过期
constexpr int Normal = 3;

class Raft : public raftRpcProctoc::raftRpc {
 private:
  std::mutex m_mtx;//互斥锁
  std::vector<std::shared_ptr<RaftRpcUtil>> m_peers;//与集群中其他节点进行rpc的接口
  std::shared_ptr<Persister> m_persister;//持久层，
  int m_me;//用与标识该节点在集群中的逻辑编号
  int m_currentTerm;//记录当前任期
  int m_votedFor;//记录当前任期给谁投过票
  std::vector<raftRpcProctoc::LogEntry> m_logs;  // 日志条目数组，包含了状态机要执行的指令集，以及收到的心跳等信息，日志条目是从1开始的，0位置是一个无效的日志条目，方便处理一些边界情况
  // 这两个状态所有结点都在维护，易失
  int m_commitIndex;
  int m_lastApplied;  // 已经汇报给状态机（上层应用）的log 的index

  // 这两个状态是由服务器来维护，易失状态
  std::vector<int> m_nextIndex;  // 这两个状态的下标1开始，因为通常commitIndex和lastApplied从0开始，应该是一个无效的index，因此下标从1开始
  std::vector<int> m_matchIndex;
  enum Status { Follower, Candidate, Leader };
  // 身份
  Status m_status;

  std::shared_ptr<LockQueue<ApplyMsg>> applyChan;  // client从这里取日志（2B），client与raft通信的接口
  // ApplyMsgQueue chan ApplyMsg // raft内部使用的chan，applyChan是用于和服务层交互，最后好像没用上

  // 选举超时

  std::chrono::_V2::system_clock::time_point m_lastResetElectionTime;
  // 心跳超时，用于leader
  std::chrono::_V2::system_clock::time_point m_lastResetHearBeatTime;

  // 2D中用于传入快照点
  // 储存了快照中的最后一个日志的Index和Term
  int m_lastSnapshotIncludeIndex;
  int m_lastSnapshotIncludeTerm;

  // 协程
  std::unique_ptr<monsoon::IOManager> m_ioManager = nullptr;

 public:
  void AppendEntries1(const raftRpcProctoc::AppendEntriesArgs *args, raftRpcProctoc::AppendEntriesReply *reply);
  void applierTicker();
  bool CondInstallSnapshot(int lastIncludedTerm, int lastIncludedIndex, std::string snapshot);
  void doElection();
  /**
   * \brief 发起心跳，只有leader才需要发起心跳
   */
  void doHeartBeat();
  // 每隔一段时间检查睡眠时间内有没有重置定时器，没有则说明超时了
  // 如果有则设置合适睡眠时间：睡眠到重置时间+超时时间
  // Leader采用先小睡一会来减少CPU空转。总体是采用睡眠的方式，来定时的查看是否需要发起选举或者发送心跳，而不是时不时的检查一下。
  void electionTimeOutTicker();
  std::vector<ApplyMsg> getApplyLogs();
  int getNewCommandIndex();
  void getPrevLogInfo(int server, int *preIndex, int *preTerm);
  void GetState(int *term, bool *isLeader);
  void InstallSnapshot(const raftRpcProctoc::InstallSnapshotRequest *args,
                       raftRpcProctoc::InstallSnapshotResponse *reply);
  void leaderHearBeatTicker();
  void leaderSendSnapShot(int server);
  void leaderUpdateCommitIndex();
  bool matchLog(int logIndex, int logTerm);
  void persist();
  void RequestVote(const raftRpcProctoc::RequestVoteArgs *args, raftRpcProctoc::RequestVoteReply *reply);
  bool UpToDate(int index, int term);
  int getLastLogIndex();
  int getLastLogTerm();
  void getLastLogIndexAndTerm(int *lastLogIndex, int *lastLogTerm);
  int getLogTermFromLogIndex(int logIndex);
  int GetRaftStateSize();
  int getSlicesIndexFromLogIndex(int logIndex);

  bool sendRequestVote(int server, std::shared_ptr<raftRpcProctoc::RequestVoteArgs> args,
                       std::shared_ptr<raftRpcProctoc::RequestVoteReply> reply, std::shared_ptr<int> votedNum);
  bool sendAppendEntries(int server, std::shared_ptr<raftRpcProctoc::AppendEntriesArgs> args,
                         std::shared_ptr<raftRpcProctoc::AppendEntriesReply> reply, std::shared_ptr<int> appendNums);

  // rf.applyChan <- msg //不拿锁执行  可以单独创建一个线程执行，但是为了同意使用std:thread
  // ，避免使用pthread_create，因此专门写一个函数来执行
  void pushMsgToKvServer(ApplyMsg msg);
  void readPersist(std::string data);
  std::string persistData();

  void Start(Op command, int *newLogIndex, int *newLogTerm, bool *isLeader);

  // Snapshot the service says it has created a snapshot that has
  // all info up to and including index. this means the
  // service no longer needs the log through (and including)
  // that index. Raft should now trim its log as much as possible.
  // index代表是快照apply应用的index,而snapshot代表的是上层service传来的快照字节流，包括了Index之前的数据
  // 这个函数的目的是把安装到快照里的日志抛弃，并安装快照数据，同时更新快照下标，属于peers自身主动更新，与leader发送快照不冲突
  // 即服务层主动发起请求raft保存snapshot里面的数据，index是用来表示snapshot快照执行到了哪条命令
  void Snapshot(int index, std::string snapshot);

  // ====================== 【扩展一】ReadIndex 读优化 ======================
#if ENABLE_READ_INDEX
  /**
   * @brief ReadIndex读优化的核心方法
   *
   * 原理：Leader收到读请求时，不走日志复制（Start），而是：
   *   1. 记录当前commitIndex作为readIndex
   *   2. 向多数节点发送一轮心跳，确认自己仍是合法Leader
   *   3. 等待lastApplied >= readIndex后，直接读取状态机
   *
   * 这避免了Get请求产生日志条目，大幅降低读延迟和IO开销。
   * 参考：etcd/TiKV 的 ReadIndex 实现。
   *
   * @param readIndex [out] 返回当前的commitIndex作为读取的安全点
   * @return true: 当前节点是Leader且已确认Leadership; false: 不是Leader
   */
  bool ReadIndex(int *readIndex);

  /**
   * @brief 等待lastApplied追上指定的readIndex
   * @param readIndex 需要等待追上的日志索引
   * @param timeoutMs 超时时间（毫秒）
   * @return true: lastApplied已追上readIndex; false: 超时
   */
  bool WaitForApplied(int readIndex, int timeoutMs);
#endif

 public:
  // 重写基类方法,因为rpc远程调用真正调用的是这个方法
  //序列化，反序列化等操作rpc框架都已经做完了，因此这里只需要获取值然后真正调用本地方法即可。
  void AppendEntries(google::protobuf::RpcController *controller, const ::raftRpcProctoc::AppendEntriesArgs *request,
                     ::raftRpcProctoc::AppendEntriesReply *response, ::google::protobuf::Closure *done) override;
  void InstallSnapshot(google::protobuf::RpcController *controller,
                       const ::raftRpcProctoc::InstallSnapshotRequest *request,
                       ::raftRpcProctoc::InstallSnapshotResponse *response, ::google::protobuf::Closure *done) override;
  void RequestVote(google::protobuf::RpcController *controller, const ::raftRpcProctoc::RequestVoteArgs *request,
                   ::raftRpcProctoc::RequestVoteReply *response, ::google::protobuf::Closure *done) override;

 public:
  void init(std::vector<std::shared_ptr<RaftRpcUtil>> peers, int me, std::shared_ptr<Persister> persister,
            std::shared_ptr<LockQueue<ApplyMsg>> applyCh);

 private:
  // for persist

  class BoostPersistRaftNode {
   public:
    friend class boost::serialization::access;
    // When the class Archive corresponds to an output archive, the
    // & operator is defined similar to <<.  Likewise, when the class Archive
    // is a type of input archive the & operator is defined similar to >>.
    template <class Archive>
    void serialize(Archive &ar, const unsigned int version) {
      ar &m_currentTerm;
      ar &m_votedFor;
      ar &m_lastSnapshotIncludeIndex;
      ar &m_lastSnapshotIncludeTerm;
      ar &m_logs;
    }
    int m_currentTerm;
    int m_votedFor;
    int m_lastSnapshotIncludeIndex;
    int m_lastSnapshotIncludeTerm;
    std::vector<std::string> m_logs;
    std::unordered_map<std::string, int> umap;

   public:
  };
};

#endif  // RAFT_H