#ifndef CONFIG_H
#define CONFIG_H

const bool Debug = true;

const int debugMul = 1;
const int HeartBeatTimeout = 25 * debugMul;
const int ApplyInterval = 10 * debugMul;

const int minRandomizedElectionTime = 300 * debugMul;  // ms
const int maxRandomizedElectionTime = 500 * debugMul;  // ms

const int CONSENSUS_TIMEOUT = 500 * debugMul;  // ms

// Feature switches. CMake can override these definitions with -D options.
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

// TTL cleanup parameters.
const int TTL_CLEANUP_INTERVAL_MS = 100;
const int TTL_CLEANUP_SAMPLE_COUNT = 20;

// Active TTL cleanup is disabled by default because deleting keys from the
// background thread would bypass Raft log replication. Expired keys are still
// filtered by read-time lazy expiration.
#ifndef ENABLE_TTL_ACTIVE_EXPIRE
#define ENABLE_TTL_ACTIVE_EXPIRE 0
#endif

// RPC thread-pool size.
const int THREAD_POOL_SIZE = 4;

// Metrics dump interval.
const int METRICS_DUMP_INTERVAL_MS = 10000;

// KV read cache capacity. The cache is a derived optimization and is not
// serialized into snapshots.
const int LRU_CACHE_CAPACITY = 1024;

// Minimal LSM memtable flush threshold, counted by distinct keys in memtable.
const int LSM_MEMTABLE_FLUSH_THRESHOLD = 1024;

// Fiber scheduler parameters.
const int FIBER_THREAD_NUM = 1;
const bool FIBER_USE_CALLER_THREAD = false;

#endif  // CONFIG_H
