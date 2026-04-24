# KV-Raft-Storage 使用与测试说明

这份文档按当前项目的实际实现编写，重点说明如何编译、运行和做基础性能测试。

## 1. 项目当前实现

本项目是一个基于 Raft 的分布式 KV 存储系统，当前主要包含：

- Raft：选举、日志复制、提交、快照安装、持久化。
- KV 状态机：支持 `Put`、`Get`、`Append`。
- 请求去重：使用 `ClientId + RequestId` 防止重试导致重复执行。
- ReadIndex：默认开启，用于优化线性一致读。
- TTL：默认开启，采用读时惰性过期；后台主动删除默认关闭。
- 存储引擎抽象：`KVEngine`。
- 默认存储引擎：`SkipListEngine`。
- LRU 缓存：默认开启，作为 `CachedKVEngine` 装饰器。
- LSM-tree：已有最小实现，默认关闭，需要构建时显式开启。
- 压测客户端：`benchMain`，用于并发请求、延迟分位和 QPS 统计。

当前默认路径是：

```text
KvServer -> CachedKVEngine -> SkipListEngine
```

如果开启 LSM：

```text
KvServer -> CachedKVEngine -> LSMTreeEngine
```

注意：当前 `KvServer` 接入 LSM 时，没有把 LSM 自身 WAL/Manifest 作为恢复真源，也没有默认启动 LSM 后台 worker。系统恢复仍主要依赖 Raft 持久化和 Raft snapshot。

## 2. 运行环境

项目面向 Linux / Ubuntu / WSL2(Ubuntu) 环境，不建议在原生 Windows 上直接运行集群。

需要依赖：

- CMake 3.22 或更高版本
- 支持 C++20 的编译器
- Protobuf 开发库
- Boost.Serialization
- Muduo
- pthread / dl
- POSIX 运行环境

代码中使用了 `fork`、`pause`、`unistd.h`、`epoll`、`pthread`、Muduo 等 Linux/POSIX 组件。

## 3. 功能开关

顶层 `CMakeLists.txt` 提供这些开关：

| 开关 | 默认值 | 说明 |
|---|---:|---|
| `ENABLE_READ_INDEX` | `ON` | 开启 ReadIndex 读优化 |
| `ENABLE_KEY_TTL` | `ON` | 开启 key TTL |
| `ENABLE_TTL_ACTIVE_EXPIRE` | `OFF` | 只开启非删除式 TTL 观察线程 |
| `ENABLE_THREAD_POOL` | `ON` | 开启 RPC 业务线程池 |
| `ENABLE_METRICS` | `ON` | 开启运行指标输出 |
| `ENABLE_LRU_CACHE` | `ON` | 开启 LRU 读缓存 |
| `ENABLE_LSM_TREE` | `OFF` | 使用 LSM-tree 存储引擎 |
| `ENABLE_DEBUG_LOG` | `OFF` | 开启详细调试日志 |

性能测试时建议保持：

```bash
-DCMAKE_BUILD_TYPE=Release -DENABLE_DEBUG_LOG=OFF
```

## 4. 编译

建议使用 out-of-source build。

```bash
cd /path/to/KV-Raft-Storage
mkdir -p build
cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j"$(nproc)"
```

可执行文件会输出到项目根目录的 `bin/`。

## 5. 推荐测试矩阵

为了让测试结果可解释，建议按固定矩阵测试，不要把多个变量混在一起。

### 5.1 基线组

关闭 TTL、LRU、LSM：

```bash
cmake .. \
  -DCMAKE_BUILD_TYPE=Release \
  -DENABLE_DEBUG_LOG=OFF \
  -DENABLE_KEY_TTL=OFF \
  -DENABLE_LRU_CACHE=OFF \
  -DENABLE_LSM_TREE=OFF
make -j"$(nproc)"
```

### 5.2 缓存组

只开启 LRU：

```bash
cmake .. \
  -DCMAKE_BUILD_TYPE=Release \
  -DENABLE_DEBUG_LOG=OFF \
  -DENABLE_KEY_TTL=OFF \
  -DENABLE_LRU_CACHE=ON \
  -DENABLE_LSM_TREE=OFF
make -j"$(nproc)"
```

### 5.3 LSM 组

只开启 LSM：

```bash
cmake .. \
  -DCMAKE_BUILD_TYPE=Release \
  -DENABLE_DEBUG_LOG=OFF \
  -DENABLE_KEY_TTL=OFF \
  -DENABLE_LRU_CACHE=OFF \
  -DENABLE_LSM_TREE=ON
make -j"$(nproc)"
```

### 5.4 全功能组

开启 TTL、LRU、LSM：

```bash
cmake .. \
  -DCMAKE_BUILD_TYPE=Release \
  -DENABLE_DEBUG_LOG=OFF \
  -DENABLE_KEY_TTL=ON \
  -DENABLE_LRU_CACHE=ON \
  -DENABLE_LSM_TREE=ON
make -j"$(nproc)"
```

## 6. 启动集群

进入 `bin/` 目录启动 3 节点集群：

```bash
cd /path/to/KV-Raft-Storage/bin
./raftCoreRun -n 3 -f bench.conf
```

这条命令会：

1. 清空并重写 `bench.conf`。
2. 通过 `fork()` 创建 3 个 Raft KV 节点。
3. 每个节点启动自己的 RPC 服务。
4. 节点地址写入 `bench.conf`。
5. 集群内部读取同一份配置文件并建立 Raft RPC 连接。

客户端必须读取同一个配置文件：

```bash
./callerMain -f bench.conf
```

## 7. 普通客户端

`callerMain` 适合做功能验证，不适合做严肃压测。

```bash
./callerMain -c 1000 -o put -k x -f bench.conf
./callerMain -c 1000 -o get -k x -f bench.conf
./callerMain -c 1000 -o both -k x -f bench.conf
./callerMain -c 100 -o ttl -k session -t 5000 -f bench.conf
```

## 8. 压测客户端

`benchMain` 是新增的压测入口，支持并发线程、key 分布、value 大小、读写比例、预热和 JSON 输出。

查看帮助：

```bash
./benchMain --help
```

常用参数：

| 参数 | 说明 |
|---|---|
| `-c <count>` | 总请求数 |
| `-j <threads>` | 客户端并发线程数 |
| `-o <op>` | 操作类型：`put/get/append/both/ttl` |
| `-m <mode>` | key 分布：`hot/unique/range` |
| `-k <key>` | hot 模式下的 key |
| `-p <prefix>` | unique/range 模式下的 key 前缀 |
| `-g <range>` | range 模式下的 key 数量 |
| `-s <size>` | value 大小，单位字节 |
| `-r <ratio>` | both 模式下读比例，0-100 |
| `-t <ttl_ms>` | ttl 模式下的 TTL |
| `-f <conf>` | 集群配置文件 |
| `--warmup <N>` | 正式统计前的预热请求数 |
| `--json` | 输出 JSON |

### 8.1 写入压测

不同 key 写入：

```bash
./benchMain -c 100000 -j 32 -o put -m unique -p bench -s 256 -f bench.conf --json
```

热点 key 写入：

```bash
./benchMain -c 100000 -j 32 -o put -m hot -k hot -s 256 -f bench.conf --json
```

### 8.2 读取压测

先预写热点 key：

```bash
./callerMain -c 1000 -o put -k readbench -f bench.conf
```

再压测读：

```bash
./benchMain -c 100000 -j 32 -o get -m hot -k readbench -f bench.conf --json
```

### 8.3 混合读写

80% 读，20% 写：

```bash
./benchMain -c 100000 -j 32 -o both -m range -p mix -g 10000 -r 80 -s 256 -f bench.conf --json
```

### 8.4 TTL 写入

```bash
./benchMain -c 50000 -j 16 -o ttl -m unique -p ttl -s 128 -t 5000 -f bench.conf --json
```

## 9. 存储层单元测试

```bash
cd /path/to/KV-Raft-Storage/build
ctest --output-on-failure
```

当前 CTest 主要覆盖：

- `storage_cache_test`
- `storage_lsm_test`

也可以直接运行：

```bash
cd /path/to/KV-Raft-Storage/bin
./storage_cache_test
./storage_lsm_test
```

## 10. 与 etcd 做基础对比

建议先把本项目和 etcd 做端到端 KV 对比，而不是直接声称是纯 Raft 层对比。

对比维度：

- 3 节点写吞吐
- 3 节点读吞吐
- 混合读写吞吐
- 平均延迟、P95、P99
- leader 故障恢复时间

本项目当前 `benchMain` 测到的是：

```text
RPC + Raft + 状态机 + KVEngine
```

不是纯 Raft。若后续要测纯 Raft 提交路径，需要再增加不修改 KV 状态的 `noop` 操作。

## 11. 测试结果记录模板

| 场景 | 构建 | TTL | LRU | LSM | 操作 | 并发 | value 大小 | QPS | 平均延迟 | P95 | P99 |
|---|---|---:|---:|---:|---|---:|---:|---:|---:|---:|---:|
| baseline put | Release | 关 | 关 | 关 | put | 32 | 256 |  |  |  |  |
| cache get | Release | 关 | 开 | 关 | get | 32 | 256 |  |  |  |  |
| lsm put | Release | 关 | 关 | 开 | put | 32 | 256 |  |  |  |  |
| full mixed | Release | 开 | 开 | 开 | both | 32 | 256 |  |  |  |  |

## 12. 当前测试边界

- `benchMain` 是端到端压测客户端，不是纯 Raft microbenchmark。
- 当前没有新增 `noop` 协议，因此不能把结果直接解释为纯 Raft 层性能。
- 当前 LSM 在 `KvServer` 接入路径下仍偏前台 flush，P99 抖动可能和 flush 有关。
- 当前集群启动方式仍以 `raftCoreRun -n 3` 自动 fork 为主，精确故障注入可以后续再增加单节点启动模式。
