# KV-Raft-Storage

这是一个基于 Raft 的分布式 KV 存储项目，当前实现包含 Raft 日志复制、KV 状态机、ReadIndex 读优化、TTL、LRU 缓存和可选 LSM-tree 存储引擎。

更完整的中文编译、运行和压测说明见：

- [MyReadMe/origin/README.md](MyReadMe/origin/README.md)

## 快速编译

项目面向 Linux / Ubuntu / WSL2(Ubuntu)，不建议在原生 Windows 上直接运行集群。

```bash
cd /path/to/KV-Raft-Storage
mkdir -p build
cd build
cmake .. -DCMAKE_BUILD_TYPE=Release -DENABLE_DEBUG_LOG=OFF
make -j"$(nproc)"
```

可执行文件输出到项目根目录的 `bin/`。

## 启动 3 节点集群

```bash
cd /path/to/KV-Raft-Storage/bin
./raftCoreRun -n 3 -f bench.conf
```

新开一个终端运行客户端：

```bash
cd /path/to/KV-Raft-Storage/bin
./callerMain -c 1000 -o both -k x -f bench.conf
```

## 性能测试

新增的 `benchMain` 用于端到端压测：

```bash
./benchMain -c 100000 -j 32 -o put -m unique -p bench -s 256 -f bench.conf --json
```

常用测试矩阵：

```bash
# 基线：关闭 TTL、LRU、LSM
cmake .. -DCMAKE_BUILD_TYPE=Release -DENABLE_DEBUG_LOG=OFF -DENABLE_KEY_TTL=OFF -DENABLE_LRU_CACHE=OFF -DENABLE_LSM_TREE=OFF

# 只测 LRU
cmake .. -DCMAKE_BUILD_TYPE=Release -DENABLE_DEBUG_LOG=OFF -DENABLE_KEY_TTL=OFF -DENABLE_LRU_CACHE=ON -DENABLE_LSM_TREE=OFF

# 只测 LSM
cmake .. -DCMAKE_BUILD_TYPE=Release -DENABLE_DEBUG_LOG=OFF -DENABLE_KEY_TTL=OFF -DENABLE_LRU_CACHE=OFF -DENABLE_LSM_TREE=ON

# 全功能
cmake .. -DCMAKE_BUILD_TYPE=Release -DENABLE_DEBUG_LOG=OFF -DENABLE_KEY_TTL=ON -DENABLE_LRU_CACHE=ON -DENABLE_LSM_TREE=ON
```

## 当前边界

- `benchMain` 测的是端到端路径：`RPC + Raft + 状态机 + KVEngine`。
- 当前没有新增 `noop` 协议，因此压测结果不能直接解释为纯 Raft 层性能。
- 当前 `KvServer` 接入 LSM 时没有把 LSM WAL/Manifest 作为恢复真源，也没有默认启动 LSM 后台 worker。
- 如果高并发压测出现 `recv error! errno:11`，通常是 RPC 客户端等待响应超时。可以降低 `-j` 并发逐步压测，或用 `-DRPC_CLIENT_TIMEOUT_MS=10000` 增大客户端超时时间。
- 详细说明以 [MyReadMe/origin/README.md](MyReadMe/origin/README.md) 为准。
