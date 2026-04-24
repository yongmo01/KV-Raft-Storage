#ifndef CONFIG_H
#define CONFIG_H

#ifndef ENABLE_DEBUG_LOG
#define ENABLE_DEBUG_LOG 0
#endif

const bool Debug = ENABLE_DEBUG_LOG;

const int debugMul = 1;
const int HeartBeatTimeout = 25 * debugMul;
const int ApplyInterval = 10 * debugMul;

const int minRandomizedElectionTime = 300 * debugMul;  // ms
const int maxRandomizedElectionTime = 500 * debugMul;  // ms

const int CONSENSUS_TIMEOUT = 500 * debugMul;  // ms

// 功能开关；CMake 可以通过 -D 参数覆盖这些默认值。
#ifndef ENABLE_READ_INDEX
#define ENABLE_READ_INDEX 1
#endif

#ifndef ENABLE_KEY_TTL
#define ENABLE_KEY_TTL 1
#endif

#ifndef ENABLE_THREAD_POOL
#define ENABLE_THREAD_POOL 1
#endif

#ifndef ENABLE_METRICS
#define ENABLE_METRICS 1
#endif

#ifndef ENABLE_LRU_CACHE
#define ENABLE_LRU_CACHE 1
#endif

#ifndef ENABLE_LSM_TREE
#define ENABLE_LSM_TREE 0
#endif

#ifndef RPC_CLIENT_TIMEOUT_MS
#define RPC_CLIENT_TIMEOUT_MS 5000
#endif

// TTL 清理参数。
const int TTL_CLEANUP_INTERVAL_MS = 100;
const int TTL_CLEANUP_SAMPLE_COUNT = 20;

// 默认关闭主动过期清理，因为后台线程直接删 key 会绕过 Raft 日志复制。
// 过期 key 仍会在读路径中按惰性过期规则被过滤。
#ifndef ENABLE_TTL_ACTIVE_EXPIRE
#define ENABLE_TTL_ACTIVE_EXPIRE 0
#endif

// RPC 业务线程池大小。
const int THREAD_POOL_SIZE = 4;
const int RPC_CLIENT_SOCKET_TIMEOUT_MS = RPC_CLIENT_TIMEOUT_MS;

// Metrics 输出间隔。
const int METRICS_DUMP_INTERVAL_MS = 10000;

// KV 读缓存容量。缓存是派生优化状态，不进入快照。
const int LRU_CACHE_CAPACITY = 1024;

// 最小 LSM memtable flush 阈值，按 memtable 中不同 key 的数量计算。
const int LSM_MEMTABLE_FLUSH_THRESHOLD = 1024;

// Fiber 调度器参数。
const int FIBER_THREAD_NUM = 1;
const bool FIBER_USE_CALLER_THREAD = false;

#endif  // CONFIG_H 头文件保护结束
