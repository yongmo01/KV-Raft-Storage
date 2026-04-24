# Minimal Benchmark Retrofit

This is the concrete, file-by-file change plan for this repository.

## P0 Must Change

### 1. Top-level build mode

File:
- [CMakeLists.txt](C:/data/KV-Raft-Storage/CMakeLists.txt)

Current issue:
- Forces `Debug` with `set(CMAKE_BUILD_TYPE "Debug")`.

Change:
- Remove the hard-coded `Debug`.
- Use:

```cmake
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()
```

Do not change:
- existing feature options
- output directories

Reason:
- Benchmark results under forced Debug are not credible.

---

### 2. Benchmark-safe debug logging switch

Files:
- [CMakeLists.txt](C:/data/KV-Raft-Storage/CMakeLists.txt)
- [src/common/include/config.h](C:/data/KV-Raft-Storage/src/common/include/config.h)

Current issue:
- `const bool Debug = true;`

Change:
- Introduce a CMake option such as `ENABLE_DEBUG_LOG`.
- In CMake, define `ENABLE_DEBUG_LOG=0` by default for benchmark builds.
- In `config.h`, replace the hard-coded `Debug` with a macro-controlled value.

Suggested shape:

```cpp
#ifndef ENABLE_DEBUG_LOG
#define ENABLE_DEBUG_LOG 0
#endif

const bool Debug = ENABLE_DEBUG_LOG;
```

Do not change:
- existing debug print call sites unless they rely on a different guard

Reason:
- Logging distorts throughput and latency.

---

### 3. Benchmark matrix discipline

File:
- [MyReadMe/origin/README.md](C:/data/KV-Raft-Storage/MyReadMe/origin/README.md) or a benchmark-specific reference doc if you do not want to touch README immediately

Change:
- Add a fixed feature matrix:
  - baseline: `TTL=OFF, LRU=OFF, LSM=OFF`
  - cache: `TTL=OFF, LRU=ON, LSM=OFF`
  - lsm: `TTL=OFF, LRU=OFF, LSM=ON`
  - full: `TTL=ON, LRU=ON, LSM=ON`

Reason:
- Prevent mixed, uninterpretable benchmark runs.

Do not change:
- feature semantics

## P1 Strongly Recommended

### 4. Add a benchmark-only client instead of overloading the demo client

Files:
- new: `example/raftCoreExample/benchMain.cpp`
- [example/raftCoreExample/CMakeLists.txt](C:/data/KV-Raft-Storage/example/raftCoreExample/CMakeLists.txt)

Why new file instead of heavily changing `callerMain`:
- keeps the demo path intact
- isolates benchmark code
- avoids turning `callerMain` into a large harness

`benchMain.cpp` should support:
- `-c <count>` total ops
- `-j <threads>` client concurrency
- `-o <put|get|both|ttl|noop>`
- `-k <key>`
- `-p <prefix>`
- `-m <hot|unique|range>` key distribution
- `-s <value_size>`
- `-r <read_ratio>` for mixed mode
- `-t <ttl_ms>`
- `-f <conf_file>`
- `--warmup <N>`
- `--json`

Do not change:
- existing `callerMain` behavior

Reason:
- Current `callerMain` is a sequential loop and cannot serve as a serious benchmark harness.

---

### 5. Add structured benchmark output

Files:
- `example/raftCoreExample/benchMain.cpp`

Output fields:
- total requests
- succeeded
- failed
- elapsed ms
- QPS
- avg latency
- p50
- p95
- p99
- wrong-leader count
- timeout count

Reason:
- Raw wall-clock only is insufficient for comparison.

## P2 Important If You Want “Raft Layer” Claims

### 6. Add a benchmark-only `noop` path

Files:
- [src/raftCore/include/kvServer.h](C:/data/KV-Raft-Storage/src/raftCore/include/kvServer.h)
- [src/raftCore/kvServer.cpp](C:/data/KV-Raft-Storage/src/raftCore/kvServer.cpp)
- [src/raftRpcPro/kvServerRPC.proto](C:/data/KV-Raft-Storage/src/raftRpcPro/kvServerRPC.proto)
- generated protobuf files if this repository still keeps them checked in

Change:
- Add an operation type such as `noop`.
- It must:
  - enter `Raft::Start()`
  - replicate and commit
  - reach apply
  - avoid mutating KV state

Reason:
- Without this, you are measuring end-to-end KV path, not “Raft layer performance”.

Do not change:
- normal `Put/Get/Append` semantics

---

### 7. Lightweight benchmark metrics

Files:
- [src/raftCore/raft.cpp](C:/data/KV-Raft-Storage/src/raftCore/raft.cpp)
- [src/raftCore/kvServer.cpp](C:/data/KV-Raft-Storage/src/raftCore/kvServer.cpp)
- [src/storage/include/cached_kv_engine.h](C:/data/KV-Raft-Storage/src/storage/include/cached_kv_engine.h)
- [src/storage/include/lsm_tree_engine.h](C:/data/KV-Raft-Storage/src/storage/include/lsm_tree_engine.h)

Add only a small set:

Raft:
- `raft_start_total`
- `raft_commit_total`
- `append_entries_sent_total`
- `read_index_total`
- `read_index_fail_total`
- `install_snapshot_total`

KV:
- `kv_put_total`
- `kv_append_total`
- `kv_get_total`
- `kv_wrong_leader_total`
- `kv_duplicate_total`

Storage:
- `cache_hit_total`
- `cache_miss_total`
- `lsm_flush_total`
- `lsm_compaction_total`
- `lsm_sstable_count`

Reason:
- You need enough observability to explain benchmark behavior.

Do not change:
- metric system scope; do not introduce Prometheus or a full telemetry stack just for this.

## P3 Fault and Topology Control

### 8. Add single-node launch mode

File:
- [example/raftCoreExample/raftKvDB.cpp](C:/data/KV-Raft-Storage/example/raftCoreExample/raftKvDB.cpp)

Current issue:
- `raftCoreRun -n 3 -f test.conf` forks multiple child processes in one command.

Change:
- Keep the existing mode.
- Add a benchmark mode such as:
  - `--single`
  - `--node-id`
  - `--port`
  - `--conf-file`

Reason:
- Easier leader kill tests
- Easier per-process CPU and RSS observation
- Easier comparison with etcd-style deployment

Do not change:
- existing auto-fork demo path

---

### 9. Add a tiny leader/state query helper

Files:
- [src/raftCore/include/raft.h](C:/data/KV-Raft-Storage/src/raftCore/include/raft.h)
- [src/raftCore/raft.cpp](C:/data/KV-Raft-Storage/src/raftCore/raft.cpp)
- optional RPC exposure if you want remote query

Expose:
- current term
- whether this node is leader
- current leader id if known

Reason:
- Required for accurate failover tests.

Do not change:
- election logic

## P4 Storage-Path Visibility Only

### 10. Expose LSM flush and compaction stats

File:
- [src/storage/include/lsm_tree_engine.h](C:/data/KV-Raft-Storage/src/storage/include/lsm_tree_engine.h)

Add:
- flush count
- flush cumulative latency
- compaction count
- compaction cumulative latency
- current memtable size
- current SSTable count

Reason:
- Current LSM path flushes in the foreground under the KvServer integration. You need direct evidence when explaining latency spikes.

Do not change yet:
- full LSM architecture

---

### 11. Optional benchmark-only background flush toggle

Files:
- [CMakeLists.txt](C:/data/KV-Raft-Storage/CMakeLists.txt)
- [src/common/include/config.h](C:/data/KV-Raft-Storage/src/common/include/config.h)
- [src/raftCore/kvServer.cpp](C:/data/KV-Raft-Storage/src/raftCore/kvServer.cpp)

Add:
- `ENABLE_LSM_BACKGROUND_FLUSH`

Behavior:
- default `OFF`
- if `ON`, `KvServer` starts LSM background worker when the engine is LSM

Reason:
- Good A/B test for foreground flush vs background flush.

Do not change:
- recovery source-of-truth semantics

## Things To Explicitly Avoid In A Benchmark Retrofit

Do not do these in the first benchmark branch:
- Multi-Raft
- MVCC
- full read/write lock redesign in `KvServer`
- RPC stack replacement
- switching to RocksDB
- changing client-visible semantics

## Minimum Useful Retrofit

If the user wants the smallest possible benchmark branch, do only:

1. `CMakeLists.txt`: default to `Release`
2. `config.h` + CMake: debug-log switch
3. add `benchMain.cpp`
4. add `noop` benchmark op
5. add single-node launch mode
6. add a handful of counters for Raft and LSM

That is enough to produce a benchmark table without changing the project beyond recognition.
