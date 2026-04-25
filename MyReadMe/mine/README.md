# KV-Raft-Storage 扩展功能详解

> 基于 Raft 共识算法的分布式 KV 存储系统的四项扩展实现

---

## 目录

- [项目总体架构](#项目总体架构)
- [扩展一：ReadIndex 读优化](#扩展一readindex-读优化)
- [扩展二：Key TTL 过期删除机制](#扩展二key-ttl-过期删除机制)
- [扩展三：RPC 业务线程池](#扩展三rpc-业务线程池)
- [扩展四：运行时 Metrics 监控](#扩展四运行时-metrics-监控)
- [功能开关与编译配置](#功能开关与编译配置)
- [构建与部署](#构建与部署)
- [模块交互全景图](#模块交互全景图)
- [文件变更清单](#文件变更清单)
- [测试与验证](#测试与验证)
- [性能与资源预估](#性能与资源预估)
- [更新日志](#更新日志)

---

## 项目总体架构

```
┌────────────┐
│   Client   │  (Clerk: Get / Put / Append / PutWithTTL)
└─────┬──────┘
      │  RPC (Protobuf + Muduo)
      ▼
┌────────────────────────────────────────────────────┐
│                  RpcProvider                        │
│  ┌──────────────┐   ┌───────────────────────────┐  │
│  │  Muduo IO    │──▶│  ThreadPool（扩展三）      │  │
│  │  Threads     │   │  异步执行 RPC 业务逻辑    │  │
│  └──────────────┘   └───────────────────────────┘  │
└──────────────────────────┬─────────────────────────┘
                           │
              ┌────────────▼────────────┐
              │       KvServer          │
              │  ┌─────────────────┐    │
              │  │ Metrics（扩展四）│    │
              │  │ TTL Map（扩展二）│    │
              │  │ ReadIndex（扩展一）│  │
              │  └─────────────────┘    │
              │           │             │
              │     ┌─────▼─────┐       │
              │     │   Raft    │       │
              │     │ (共识层)  │       │
              │     └─────┬─────┘       │
              │           │             │
              │     ┌─────▼──────┐      │
              │     │  SkipList  │      │
              │     │  (存储引擎) │     │
              │     └─────┬──────┘      │
              │           │             │
              │     ┌─────▼──────┐      │
              │     │ Persister  │      │
              │     │ (持久化)   │      │
              │     └────────────┘      │
              └─────────────────────────┘
```

### 五层模块说明

| 层次 | 模块 | 职责 |
|------|------|------|
| 网络层 | `RpcProvider` / `MprpcChannel` | 基于 Muduo 的 TCP 通信、Protobuf 序列化 |
| 共识层 | `Raft` | Leader 选举、日志复制、快照、安全性保证 |
| 状态机层 | `KvServer` | 将已提交的日志应用到 KV 存储，处理客户端请求 |
| 存储引擎 | `SkipList` | 基于跳表的高效 KV 存储（$O(\log n)$ 读写） |
| 持久化层 | `Persister` | Raft 状态和快照的持久化存储 |

---

## 扩展一：ReadIndex 读优化

### 1.1 背景与动机

在标准 Raft 实现中，读请求也需要走**完整的日志复制流程**（`Start()` → 日志复制 → 提交 → 应用），这带来了不必要的开销：

- 读请求产生日志条目，增大日志体积和磁盘 IO
- 读延迟等于一次完整共识的延迟（通常 10-50ms）
- 在读多写少的场景中，读操作白白占用了大量带宽

### 1.2 设计原理

ReadIndex 协议（参考 etcd / TiKV 的实现）的核心思路：

1. **Leader 收到读请求**，记录当前 `commitIndex` 作为 `readIndex`
2. **发起一轮心跳**，确认自己仍是合法 Leader（防止网络分区导致的 stale read）
3. **等待 `lastApplied >= readIndex`**，确保状态机已经应用到安全点
4. **直接从状态机读取数据**，无需产生日志条目

### 1.3 代码实现

#### 核心方法 — `Raft::ReadIndex()`

```
文件: src/raftCore/raft.cpp
```

```cpp
bool Raft::ReadIndex(int *readIndex) {
    // 1. 检查 Leader 身份，记录 commitIndex 作为 readIndex
    // 2. 向所有 Follower 发送心跳（空 AppendEntries）
    // 3. 使用 atomic 计数器等待多数派确认
    // 4. 最终再次检查 term 一致性
}
```

**关键设计点**：
- 使用 `std::atomic<int>` 而非 `std::mutex` 来统计心跳确认数，避免锁竞争
- 发送心跳前释放大锁（`m_mtx`），允许心跳并行发送
- 超时使用 `CONSENSUS_TIMEOUT`（默认 500ms）作为上限
- 最终检查 term 一致性，防止已经不是 Leader 后返回脏数据

#### 等待状态机追上 — `Raft::WaitForApplied()`

```cpp
bool Raft::WaitForApplied(int readIndex, int timeoutMs) {
    // 轮询 m_lastApplied >= readIndex
    // 1ms 粒度的短睡眠避免 busy-wait
}
```

#### KvServer 集成 — `KvServer::Get()`

```cpp
void KvServer::Get(/* ... */) {
    // 先尝试 ReadIndex 快速路径
    int readIndex = 0;
    if (m_raftNode->ReadIndex(&readIndex)) {
        if (m_raftNode->WaitForApplied(readIndex, CONSENSUS_TIMEOUT)) {
            // 直接从 SkipList 读取，无需日志复制
            return;
        }
    }
    // ReadIndex 失败，降级为标准日志复制路径
    m_raftNode->Start(op, &raftIndex, &requestTerm, &isLeader);
}
```

### 1.4 优势分析

| 指标 | 原始实现 | ReadIndex 优化后 |
|------|---------|-----------------|
| 读延迟 | 一次完整共识 (~25ms) | 一轮心跳 (~5ms) |
| 读产生日志 | 每次读一条 | 零 |
| 磁盘 IO | 有 | 无 |
| 降级方案 | — | 自动降级为日志复制 |

---

## 扩展二：Key TTL 过期删除机制

### 2.1 背景与动机

原始系统不支持键值对的自动过期，无法满足以下常见场景：
- 分布式会话（Session）自动失效
- 缓存数据定时刷新
- 临时锁的自动释放

### 2.2 设计原理

采用 **Redis 风格的双删策略**：

1. **惰性删除**（Lazy Deletion）：读取 Key 时检查是否过期，过期则删除并返回空
2. **定期清理**（Active Expiry）：后台线程定期随机抽样检查过期 Key 并删除

### 2.3 数据结构

```cpp
// 存储每个 Key 的过期时间戳（毫秒）
// Key: 键名, Value: 绝对过期时间戳 (ms since epoch)
std::unordered_map<std::string, int64_t> m_expireMap;
```

TTL 信息通过 Raft 日志复制保证集群一致性：
- `Op` 结构体新增 `TtlMs` 字段
- `PutAppendArgs` Protobuf 消息新增 `int64 TtlMs = 6`
- TTL 信息跟随 Put/Append 日志在所有节点同步

### 2.4 代码实现

#### 惰性删除 — `ExecuteGetOpOnKVDB()`

```cpp
void KvServer::ExecuteGetOpOnKVDB(Op op, ...) {
    if (isKeyExpired(op.Key)) {
        m_skipList.delete_element(op.Key);
        m_expireMap.erase(op.Key);
        *value = "";
        *exist = false;
        return;
    }
    // 正常读取...
}
```

#### 定期清理 — `activeExpireCycle()`

```cpp
void KvServer::activeExpireCycle() {
    while (true) {
        // 每 TTL_CLEANUP_INTERVAL_MS(100ms) 执行一次
        // 随机抽样 TTL_CLEANUP_SAMPLE_COUNT(20) 个 Key
        // 删除已过期的 Key
        // 如果抽样中过期比例 > 25%，立即再次执行
    }
}
```

#### 客户端接口

```cpp
// Clerk 新增带 TTL 的接口
clerk.PutWithTTL("session:abc", "user-data", 30000);   // 30秒过期
clerk.AppendWithTTL("log:123", "entry", 60000);         // 60秒过期
clerk.Put("permanent-key", "value");                     // 永不过期（TTL=0）
```

### 2.5 一致性保证

```
Client                  Leader KvServer              Follower KvServer
  │                          │                              │
  │ PutWithTTL(k,v,TTL=30s) │                              │
  │─────────────────────────▶│                              │
  │                          │ Op{Key=k, Value=v, TtlMs=30000}
  │                          │── 日志复制 ─────────────────▶│
  │                          │                              │ 应用日志 → 设置 expireMap[k]
  │                          │ 应用日志 → 设置 expireMap[k] │
  │                          │                              │
```

所有节点通过相同的日志应用相同的 TTL 设置，保证集群一致的过期行为。

---

## 扩展三：RPC 业务线程池

### 3.1 背景与动机

原始实现中，Muduo 的 IO 线程既负责网络 IO 又负责执行 RPC 业务逻辑。当业务逻辑耗时较长时（如 Raft 共识等待），IO 线程被阻塞，导致：

- 新连接的请求排队等待
- 心跳响应延迟，可能触发不必要的选举
- 吞吐量下降

### 3.2 设计原理

引入**生产者-消费者模式**的线程池，将 RPC 业务逻辑从 IO 线程卸载到独立线程池：

```
Muduo IO Thread ──▶ 解析RPC请求 ──▶ ThreadPool.enqueue(业务处理)
                                         │
                                    ┌─────▼─────┐
                                    │ Worker 1   │
                                    │ Worker 2   │
                                    │ Worker 3   │
                                    │ Worker 4   │
                                    └────────────┘
                                         │
                                    完成后回调 ──▶ Muduo IO Thread 发送响应
```

### 3.3 核心实现

#### ThreadPool 类（`src/common/include/threadPool.h`）

```cpp
class ThreadPool {
    std::vector<std::thread> m_workers;        // 工作线程
    std::queue<std::function<void()>> m_tasks;  // 任务队列
    std::mutex m_mutex;                         // 队列锁
    std::condition_variable m_cv;               // 条件变量

    template<typename F, typename... Args>
    auto enqueue(F&& f, Args&&... args) -> std::future<...>;
};
```

**特性**：
- 模板化的 `enqueue()` 方法，可接受任意可调用对象
- 返回 `std::future`，支持异步获取结果
- 优雅关闭：析构时等待所有任务完成
- `pendingTasks()` 方法支持查询当前排队任务数

#### RpcProvider 集成

```cpp
// rpcprovider.cpp - OnMessage()
#if ENABLE_THREAD_POOL
  m_businessThreadPool.enqueue([service, method, request, response, done]() {
    service->CallMethod(method, nullptr, request, response, done);
  });
#else
  service->CallMethod(method, nullptr, request, response, done);
#endif
```

### 3.4 线程数配置

默认线程池大小为 `THREAD_POOL_SIZE = 4`，当前通过 CMake 参数传入，并在 `config.h` 中转换为 `RPC_THREAD_POOL_SIZE` 使用。

在 2 核 CPU 的环境下，推荐配置方式：
- IO 线程数（Muduo `setThreadNum`）：2-4
- 业务线程池大小：1-2

---

## 扩展四：运行时 Metrics 监控

### 4.1 背景与动机

分布式系统的可观测性（Observability）至关重要。没有监控数据，排查以下问题极为困难：

- 请求延迟分布和尾延迟
- 选举频率和 Leader 稳定性
- 日志复制效率

### 4.2 设计原理

实现了一个**轻量级、无第三方依赖**的监控指标系统，支持三种指标类型：

| 类型 | 用途 | 示例 |
|------|------|------|
| **Counter** | 累计计数器 | 请求总数、选举次数 |
| **Gauge** | 瞬时数值 | 当前 term、commitIndex |
| **Latency** | 延迟分布直方图 | 请求延迟 p50/p99 |

### 4.3 核心实现

#### Metrics 单例类（`src/common/include/metrics.h`）

```cpp
class Metrics {
    std::unordered_map<std::string, std::atomic<int64_t>> m_counters;
    std::unordered_map<std::string, std::atomic<int64_t>> m_gauges;
    std::unordered_map<std::string, std::vector<double>> m_latencies;

    void incCounter(const std::string& name, int64_t delta = 1);
    void setGauge(const std::string& name, int64_t value);
    void recordLatency(const std::string& name, double ms);
    std::string dump();  // 输出 Prometheus 风格的指标文本
};
```

#### RAII 延迟测量

```cpp
// 自动测量一段代码的执行耗时
{
    MetricsTimer timer("kv_get_latency_ms");
    // ... 业务代码 ...
}  // 析构时自动记录延迟
```

#### 便利宏

```cpp
METRICS_INC_COUNTER("kv_get_total");           // 计数器 +1
METRICS_SET_GAUGE("raft_current_term", 5);     // 设置瞬时值
METRICS_TIMER("kv_get_latency_us");            // RAII 延迟测量（作用域结束自动记录）
METRICS_RECORD_LATENCY("rpc_latency", 12.5);  // 直接记录延迟值
```

当 `ENABLE_METRICS=0` 时，所有宏展开为空操作，零运行时开销。

### 4.4 监控指标列表

#### KvServer 层

| 指标名 | 类型 | 含义 |
|--------|------|------|
| `kv_get_total` | Counter | Get 请求总数 |
| `kv_putappend_total` | Counter | Put/Append 请求总数 |
| `kv_get_latency_us` | Latency | Get 请求端到端延迟（微秒） |
| `kv_putappend_latency_us` | Latency | PutAppend 请求端到端延迟（微秒） |

#### Raft 层

| 指标名 | 类型 | 含义 |
|--------|------|------|
| `raft_election_started` | Counter | 发起选举次数 |
| `raft_election_won` | Counter | 当选 Leader 次数 |
| `raft_current_term` | Gauge | 当前任期 |
| `raft_commit_index` | Gauge | 已提交日志索引 |
| `raft_last_applied` | Gauge | 已应用日志索引 |
| `raft_log_size` | Gauge | 内存中日志条目数 |
| `raft_append_entries_latency_us` | Latency | AppendEntries RPC 延迟（微秒） |

### 4.5 输出示例

KvServer 后台线程每 `METRICS_DUMP_INTERVAL_MS`（默认 10 秒）输出一次：

```
======== Metrics Dump ========
[Counter] kv_get_total = 1523
[Counter] kv_putappend_total = 487
[Counter] raft_election_started = 2
[Counter] raft_election_won = 1
[Gauge] raft_current_term = 3
[Gauge] raft_commit_index = 2010
[Gauge] raft_last_applied = 2010
[Gauge] raft_log_size = 156
[Latency] kv_get_latency_us: count=1523, p50=2300us, p99=15800us
[Latency] kv_putappend_latency_us: count=487, p50=8500us, p99=45200us
[Latency] raft_append_entries_latency_us: count=3890, p50=1200us, p99=8900us
================================
```

---

## 功能开关与编译配置

### 编译时开关

所有扩展功能均支持**编译时独立开关**，通过 `#define` 宏控制，默认全部开启。

#### 配置文件位置

```
src/common/include/config.h
```

#### 宏定义

```cpp
// 扩展一：ReadIndex 读优化（1=开启, 0=关闭）
#ifndef ENABLE_READ_INDEX
#define ENABLE_READ_INDEX 1
#endif

// 扩展二：Key TTL 过期删除机制
#ifndef ENABLE_KEY_TTL
#define ENABLE_KEY_TTL 1
#endif

// 扩展三：RPC 业务线程池
#ifndef ENABLE_THREAD_POOL
#define ENABLE_THREAD_POOL 1
#endif

// 扩展四：运行时 Metrics 监控
#ifndef ENABLE_METRICS
#define ENABLE_METRICS 1
#endif
```

#### CMake 控制

```bash
# 全部开启（默认）
cmake -B build

# 关闭 ReadIndex 优化
cmake -B build -DENABLE_READ_INDEX=OFF

# 只开启 TTL 和 Metrics
cmake -B build -DENABLE_READ_INDEX=OFF -DENABLE_THREAD_POOL=OFF

# 全部关闭（退化为原始实现）
cmake -B build -DENABLE_READ_INDEX=OFF -DENABLE_KEY_TTL=OFF \
               -DENABLE_THREAD_POOL=OFF -DENABLE_METRICS=OFF
```

#### 扩展参数配置

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `TTL_CLEANUP_INTERVAL_MS` | 100 | TTL 定期清理间隔（毫秒） |
| `TTL_CLEANUP_SAMPLE_COUNT` | 20 | 每次清理抽样 Key 数 |
| `THREAD_POOL_SIZE` | 4 | 业务线程池大小，可通过 `-DTHREAD_POOL_SIZE=<N>` 调整 |
| `METRICS_DUMP_INTERVAL_MS` | 10000 | Metrics 输出间隔（毫秒） |

---

## 构建与部署

### 环境依赖

| 依赖 | 版本要求 | 用途 |
|------|---------|------|
| CMake | ≥ 3.22 | 构建系统 |
| GCC/G++ | C++20 支持 | 编译器 |
| Muduo | — | 网络库 |
| Protobuf | ≥ 3.x | RPC 序列化 |
| Boost | ≥ 1.70 | 序列化、any |

### 构建步骤

```bash
# 1. 克隆项目 (如果已在原项目中，直接进入目录即可)
# git clone <repo-url>
cd KV-Raft-Storage

# 2. 构建项目（默认开启所有扩展）
# 推荐使用 out-of-source 外部构建保持源码目录整洁
mkdir build && cd build
cmake ..
# 开启多核并行编译加速
make -j$(nproc)

# 3. 验证可执行文件
# 编译后的二进制文件及默认配置将会输出到项目根目录的 bin/ 目录
ls ../bin/
```

### 部署集群

> **⚠️ 关于配置文件的核心说明：** 
> 程序内部采用网络与端口的自动分配并写入的方式，在集群**启动时**会自动清空原配置文件 `bin/test.conf` 并重新录入最新分配的网络映射。因此，请确保在运行 `caller.cpp` 等客户端测试的时候与服务端共用该配置文件！

```bash
# 1. 切换到可执行文件目录
cd ../bin

# 2. 启动包含 3 个节点的 Raft 集群（只需一条命令！）
# 命令格式：./raftCoreRun -n <总节点数量> -f <写入的配置文件名>
#
# 内部运行机制（基于多进程）：
#   (1) raftCoreRun 首先会自动清空并重置 test.conf（这就是为什么终端会打印 "test.conf 已清空"）
#   (2) 然后循环 3 次，调用 fork() 产生三个子进程运行 KvServer。
#   (3) 运行时会自动分配并绑定可用的端口，并追加写入 test.conf 里。
# 
# 所以你在一个终端直接运行下述命令即可：
./raftCoreRun -n 3 -f test.conf

# 此时你会首先看到 "test.conf 已清空" 的提示，等待子进程初始化完毕后
# 节点之间便开始发送 RequestVote，能够成功选举出 Leader！

# 3. 启动客户端发出请求
# 请打开另外的一个新终端进入 bin 目录下，启动客户端！
# 客户端（如 callerMain）在启动时，会读取刚刚由 raftCoreRun 生成的 test.conf 并解析里面的 3 个节点地址进行请求：
./callerMain

# 客户端 callerMain 支持丰富的命令行参数，方便进行各种压测控制：
# 参数说明：
#   -c <count>      : 向服务器发起的总请求次数 (默认 500)
#   -o <operation>  : 执行的操作模式。可选值：
#                     'both' (边写边读，默认)
#                     'put'  (只疯狂写入进行压测)
#                     'get'  (只疯狂读取，用于验证 ReadIndex 等)
#                     'ttl'  (带有 TTL 过期标记的写入)
#   -k <key>        : 指定操作的 Key 的前缀名 (默认 'x')
#   -t <ttl_ms>     : 设定带 TTL 模式下的过期毫秒数 (默认 3000ms)
#   -f <conf_file>  : 读取的集群信息配置文件 (默认 'test.conf')
# 
# 使用示例：
# 1. 模拟慢磁盘阻塞测试（高强度纯 Put 压测）：
#    ./callerMain -c 50000 -o put
# 2. 模拟 ReadIndex 高性能纯读性能打流：
#    ./callerMain -c 50000 -o get -k my_key
# 3. 模拟小内存下的 TTL 兜底观察测试：
#    ./callerMain -c 10000 -o ttl -t 5000
```

---

## 模块交互全景图

### 读请求流程（ReadIndex 开启时）

```
Client                        KvServer                    Raft                    Followers
  │ Get(key)                      │                          │                        │
  │──────────────────────────────▶│                          │                        │
  │                               │ ReadIndex()              │                        │
  │                               │─────────────────────────▶│                        │
  │                               │                          │ 心跳(空AE) ──────────▶│
  │                               │                          │◀──── 确认 ─────────────│
  │                               │           readIndex      │                        │
  │                               │◀─────────────────────────│                        │
  │                               │ WaitForApplied(readIndex)│                        │
  │                               │─────────────────────────▶│ lastApplied >= readIndex?
  │                               │◀─── true ────────────────│                        │
  │                               │ 直接读 SkipList          │                        │
  │◀── value ─────────────────────│                          │                        │
```

### 写请求流程（TTL 开启时）

```
Client                        KvServer                    Raft                    Followers
  │ PutWithTTL(k,v,30s)          │                          │                        │
  │──────────────────────────────▶│                          │                        │
  │                               │ Op{Key=k,TtlMs=30000}   │                        │
  │                               │ Start(op)               │                        │
  │                               │─────────────────────────▶│                        │
  │                               │                          │ 日志复制 ─────────────▶│
  │                               │                          │◀──── 确认 ─────────────│
  │                               │ ApplyMsg(op)             │                        │
  │                               │◀─────────────────────────│                        │
  │                               │ ExecutePut + setKeyExpire│                        │
  │◀── OK ────────────────────────│                          │                        │
  │                               │                          │                        │
  │         ... 30 秒后 ...       │                          │                        │
  │                               │ [后台] activeExpireCycle │                        │
  │                               │ 检测到 key 过期，删除    │                        │
```

---

## 文件变更清单

### 新增文件

| 文件 | 说明 |
|------|------|
| `src/common/include/threadPool.h` | 线程池（Header-only） |
| `src/common/include/metrics.h` | Metrics 监控系统（Header-only） |
| `MyReadMe/mine/README.md` | 本文档 |

### 修改文件

| 文件 | 变更内容 |
|------|---------|
| `CMakeLists.txt` | 添加 cmake `option()` 功能开关 |
| `src/common/include/config.h` | 添加四个 `ENABLE_*` 宏定义和扩展参数 |
| `src/common/include/util.h` | `Op` 类添加 `TtlMs` 字段、`getCurrentTimeMs()` |
| `src/raftRpcPro/kvServerRPC.proto` | `PutAppendArgs` 添加 `TtlMs` 字段 |
| `src/raftRpcPro/include/kvServerRPC.pb.h` | 对应 Protobuf 生成代码更新 |
| `src/raftRpcPro/kvServerRPC.pb.cc` | 对应 Protobuf 生成代码更新 |
| `src/raftCore/include/raft.h` | 添加 `ReadIndex()` / `WaitForApplied()` 声明 |
| `src/raftCore/raft.cpp` | 实现 ReadIndex/WaitForApplied + Metrics 集成 |
| `src/raftCore/include/kvServer.h` | 添加 TTL 成员/方法声明、Metrics 集成 |
| `src/raftCore/kvServer.cpp` | TTL/ReadIndex/Metrics 全面集成 |
| `src/rpc/include/rpcprovider.h` | 添加 ThreadPool 成员 |
| `src/rpc/rpcprovider.cpp` | 业务处理卸载到线程池 |
| `src/raftClerk/include/clerk.h` | 添加带 TTL 的 Put/Append 接口声明 |
| `src/raftClerk/clerk.cpp` | 实现带 TTL 的 PutAppend |

---

## 测试与验证

### 1. 功能开关测试

验证每个扩展可以独立关闭且编译通过：

```bash
# 逐个关闭测试
cmake -B build -DENABLE_READ_INDEX=OFF && cmake --build build -j$(nproc)
cmake -B build -DENABLE_KEY_TTL=OFF && cmake --build build -j$(nproc)
cmake -B build -DENABLE_THREAD_POOL=OFF && cmake --build build -j$(nproc)
cmake -B build -DENABLE_METRICS=OFF && cmake --build build -j$(nproc)

# 全部关闭
cmake -B build -DENABLE_READ_INDEX=OFF -DENABLE_KEY_TTL=OFF \
               -DENABLE_THREAD_POOL=OFF -DENABLE_METRICS=OFF && cmake --build build -j$(nproc)
```

### 2. ReadIndex 测试

```
验证步骤：
1. 启动 3 节点集群
2. 发送若干写请求
3. 发送连续的 Get 请求
4. 观察日志：应看到 "[ReadIndex-rf{X}] ReadIndex成功" 而非 Start()
5. 对比有/无 ReadIndex 时的 Get 延迟（Metrics 指标 kv_get_latency_ms）
```

### 3. Key TTL 测试

```
验证步骤：
1. 客户端调用 PutWithTTL("test-key", "test-value", 5000)  // 5秒过期
2. 立即 Get("test-key") → 应返回 "test-value"
3. 等待 6 秒后 Get("test-key") → 应返回 ""
4. 观察日志：应看到 activeExpireCycle 删除过期 Key 的记录
```

### 4. 线程池测试

```
验证步骤：
1. 观察启动日志，确认线程池创建
2. 并发发送多个 RPC 请求
3. 对比有/无线程池时的请求吞吐量
4. 使用 top/htop 观察 CPU 使用分布
```

### 5. Metrics 测试

```
验证步骤：
1. 启动集群后等待 10-20 秒
2. 观察标准输出中的 "======== Metrics Dump ========" 输出
3. 发送一批 Get/Put 请求
4. 观察 Counter 值递增
5. 观察 p50/p99 延迟数据
```

---

## 性能与资源预估

### 资源占用（2GB RAM / 2核 CPU 环境）

| 扩展 | 额外内存 | CPU 影响 |
|------|---------|---------|
| ReadIndex | 几乎为零 | 减少共识 CPU 用于读请求 |
| Key TTL | `expireMap` ~10KB/万 Key | 后台线程定期运行，极低 |
| ThreadPool | 线程栈 ~32KB × 4 = 128KB | 更高效地利用多核 |
| Metrics | 滑动窗口 ~800KB | 计数/记录操作 ~ns 级 |

**总额外内存**：约 1MB，完全在 2GB 环境内运行。

### 性能提升预期

- **读延迟**：ReadIndex 使 Get 延迟降低约 50-80%（避免日志复制）
- **吞吐量**：ThreadPool 使并发 RPC 吞吐提升 30-50%（IO 线程不被阻塞）
- **存储效率**：TTL 自动清理过期数据，防止存储无限增长
- **可观测性**：Metrics 提供 p50/p99 延迟和系统状态全景

---

## 设计原则与面试要点

1. **编译时零开销**：所有扩展关闭时，通过 `#if` 预处理器完全消除代码，无运行时开销
2. **渐进式集成**：每个扩展独立，不影响原有代码路径
3. **优雅降级**：ReadIndex 失败时自动退回到日志复制方案
4. **一致性优先**：TTL 通过 Raft 日志复制，保证所有节点一致过期
5. **参考业界实践**：ReadIndex 参考 etcd/TiKV，TTL 参考 Redis，Metrics 参考 Prometheus

---

## 更新日志

### 2026-03-12 Bug 修复：客户端交互后集群崩溃

**问题现象**：启动 3 节点 Raft 集群后，执行 `./callerMain` 与集群交互。客户端能正常完成约 200 次 PutAppend/Get 操作（从 499 到 290），之后 Leader 突然丢失，集群无法选出新的稳定 Leader，所有节点间连接断开，最终进程崩溃。

**错误日志关键行**：
```
Error: [func-getSlicesIndexFromLogIndex-rf{1}]  index{210} <= rf.lastSnapshotIncludeIndex{211}
terminate called after throwing an instance of 'boost::archive::archive_exception'
  what():  invalid signature
```

**根因分析**：定位到 **3 个关联 Bug**：

#### Bug 1（致命）：ReadIndex 缺少快照边界检查

- **文件**：`src/raftCore/raft.cpp` — `Raft::ReadIndex()`
- **原因**：`ReadIndex` 为确认 Leadership 向 Follower 发送心跳时，直接调用 `getPrevLogInfo(i, ...)` 构造 AE 参数，**没有** 像 `doHeartBeat()` 那样先检查 `m_nextIndex[i] <= m_lastSnapshotIncludeIndex`。当日志被快照截断后（例如 `m_lastSnapshotIncludeIndex = 211`），若某个 peer 的 `m_nextIndex` 仍为 211（等于快照索引），`getPrevLogInfo` 会计算 `preIndex = 210`，调用 `getSlicesIndexFromLogIndex(210)` 触发断言 `index > m_lastSnapshotIncludeIndex` 失败，`myAssert` 调用 `std::exit(EXIT_FAILURE)` **导致整个进程崩溃**。
- **修复**：在 ReadIndex 的心跳线程中，加锁后立即检查 `m_nextIndex[i] <= m_lastSnapshotIncludeIndex`，若成立则跳过该 peer（`finished++` 后 return）。

#### Bug 2（重要）：AppendEntries1 缺少 return 语句

- **文件**：`src/raftCore/raft.cpp` — `Raft::AppendEntries1()`
- **原因**：`else if (args->prevlogindex() < m_lastSnapshotIncludeIndex)` 分支设置了 reply 但**没有 return**，导致 fall through 到 `matchLog(args->prevlogindex(), args->prevlogterm())`。`matchLog` 内部断言 `logIndex >= m_lastSnapshotIncludeIndex` 会失败，同样调用 `std::exit` 崩溃。这是一个**既有 Bug**（原始代码中已存在，但扩展功能增加了 RPC 频率使其更容易被触发）。
- **修复**：在该分支末尾添加 `return;`。

#### Bug 3（中等）：线程池未区分 Raft/KV RPC

- **文件**：`src/rpc/rpcprovider.cpp` — `RpcProvider::OnMessage()`
- **原因**：线程池将**所有** RPC（包括 Raft 心跳/投票和 KV Put/Get）放入同一个 4 线程的线程池。KV 操作（PutAppend/Get）会阻塞线程等待 Raft 共识（最长 500ms），池满时 Raft 心跳 RPC 被延迟处理，导致 Follower 选举超时、Leader 频繁切换。
- **修复**：通过判断 `service_name == "raftRpc"` 区分服务类型。Raft 内部 RPC 直接在 Muduo IO 线程同步执行（保证低延迟），仅将 KV 业务 RPC 异步卸载到线程池。

#### 附加修复：RPC request/response 内存泄漏

- **文件**：`src/rpc/rpcprovider.cpp`
- **原因**：`OnMessage` 中通过 `New()` 分配的 `request` 和 `response` 对象从未被释放，每次 RPC 调用都泄漏两个 protobuf Message 对象。长时间运行的集群会逐渐耗尽内存。
- **修复**：`CallMethod` 返回后 `delete request`；`SendRpcResponse` 发送完毕后 `delete response`。

**修改文件清单**：
| 文件 | 修改内容 |
|------|---------|
| `src/raftCore/raft.cpp` | ReadIndex 增加快照边界检查；AppendEntries1 增加 return |
| `src/rpc/rpcprovider.cpp` | 线程池区分 Raft/KV RPC；修复 request/response 内存泄漏 |

### 4. 修复 Raft 空载时内存持续泄漏及线程死锁问题 (已修复)
- **现象**：集群在空载（无客户端连接）状态下，服务端内存依然随时间保持线性下降，最终 OOM。
- **原因**：底层 RPC 收发通道 MprpcChannel 复用同一个短连接套接字 m_clientFd 且没有加锁同步。Raft doHeartBeat 等函数会高频启动 detached std::thread，这些并发线程同时操作同一个 Socket，造成 TCP 报文发送交错受损，以及接收时响应报文与请求不匹配。导致大量线程永远无法收到正确回复，永久阻塞在同步的 ecv() 调用上（Thread Leak），每个堆积的线程都会浪费几兆栈内存。
- **修复**：
  - 在 src/rpc/include/mprpcchannel.h 中为 Channel 增加 std::mutex m_clientFd_mutex。
  - 在 src/rpc/mprpcchannel.cpp 的 CallMethod 函数入口增加 std::lock_guard<std::mutex> lock(m_clientFd_mutex)，确保同一个 Peer 的 RPC 调用串行化。
  - 在 
ewConnect 建立 Socket 后立刻注入 SO_RCVTIMEO 和 SO_SNDTIMEO (500ms) 套接字超时保护，强制让无法收到回复的僵尸线程报错退出并释放资源。

**修改文件清单补充**：
| 文件 | 修改内容 |
|------|---------|
| src/rpc/include/mprpcchannel.h | 引入 mutex 成员以控制 m_clientFd 的单点读写 |
| src/rpc/mprpcchannel.cpp | 加入 lock_guard 并为 fd 增加 setsockopt 超时选项 |
