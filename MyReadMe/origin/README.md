# KV-Raft-Storage 实际实现说明

这份 README 只描述当前仓库里已经实现并能对得上代码的内容，不写理想设计。

## 1. 项目当前实现了什么

这个项目是一个基于 Raft 的 KV 存储系统，主体是 `Raft + RPC + KV 状态机`。当前仓库里的核心能力如下：

- Raft 基本流程：选举、日志复制、快照安装、状态持久化。
- KV 语义：`Put`、`Get`、`Append`。
- 重复请求去重：按 `ClientId + RequestId` 去重。
- ReadIndex 读优化：默认开启。
- TTL：默认开启，采用“读时惰性过期”；后台主动删除默认关闭，避免绕过 Raft 改状态机。
- 存储引擎抽象：`KVEngine`。
- 默认存储引擎：`SkipListEngine`。
- 读缓存：默认开启 `CachedKVEngine + LRUCache`。
- LSM 最小实现：仓库里已经有 `LSMTreeEngine`，但默认关闭，需要显式打开。
- Metrics：默认开启。
- RPC 线程池：默认开启；KV 业务 RPC 可卸载到线程池，Raft 内部 RPC 仍直接执行。

## 2. 默认功能开关

顶层 `CMakeLists.txt` 当前默认值如下：

- `ENABLE_READ_INDEX=ON`
- `ENABLE_KEY_TTL=ON`
- `ENABLE_TTL_ACTIVE_EXPIRE=OFF`
- `ENABLE_THREAD_POOL=ON`
- `ENABLE_METRICS=ON`
- `ENABLE_LRU_CACHE=ON`
- `ENABLE_LSM_TREE=OFF`

也就是说，默认行为是：

- 读路径走 `ReadIndex`
- KV 使用 `SkipListEngine`
- 外面包一层 LRU 缓存
- TTL 只在读时惰性清理
- 不默认启用 LSM

## 3. 运行环境要求

这个项目按当前实现，应该在 Linux 环境下编译运行。更准确地说，是面向 `Linux / Ubuntu / WSL2(Ubuntu)` 的工程，不是面向原生 Windows。

原因很直接，代码里大量使用了 Linux/POSIX 组件：

- `fork`、`pause`、`getopt`
- `unistd.h`
- `ucontext`
- `epoll`
- `pthread`
- `arpa/inet.h`、`sys/socket.h`
- Muduo 网络库

## 4. 构建依赖

需要准备这些依赖：

- CMake 3.22 或更高
- 支持 C++20 的编译器
- Protobuf 开发头文件和链接库
- Boost.Serialization
- Muduo
- `pthread`、`dl`

仓库里已经包含了生成好的 `*.pb.cc / *.pb.h`，所以正常编译不要求你先手动跑 `protoc`。只有当你修改了 `.proto` 文件时，才需要重新生成 Protobuf 代码。

## 5. 正确的编译方式

推荐使用 out-of-source build。

```bash
cd /path/to/KV-Raft-Storage

mkdir -p build
cd build

cmake ..
make -j"$(nproc)"
```

编译完成后，可执行文件会输出到项目根目录的 `bin/`，因为顶层 CMake 里设置了：

- `EXECUTABLE_OUTPUT_PATH=${PROJECT_SOURCE_DIR}/bin`

### 可选开关示例

如果你要切换功能开关，可以这样构建：

```bash
cmake .. \
  -DENABLE_READ_INDEX=ON \
  -DENABLE_KEY_TTL=ON \
  -DENABLE_TTL_ACTIVE_EXPIRE=OFF \
  -DENABLE_THREAD_POOL=ON \
  -DENABLE_METRICS=ON \
  -DENABLE_LRU_CACHE=ON \
  -DENABLE_LSM_TREE=OFF
```

如果你想试 LSM：

```bash
cmake .. -DENABLE_LSM_TREE=ON
make -j"$(nproc)"
```

## 6. 当前可执行目标

根据当前 CMake，主要目标有：

- `raftCoreRun`：启动 Raft KV 集群
- `callerMain`：KV 客户端
- `provider` / `consumer`：RPC 示例
- `test_server` / `test_scheduler` / `test_iomanager` / `test_hook`：fiber 示例
- `storage_cache_test`：LRU / CachedKVEngine 测试
- `storage_lsm_test`：LSMTreeEngine 测试

## 7. 正确的运行方式

### 7.1 启动 Raft KV 集群

先进入 `bin/` 目录再启动，这一点很重要。

```bash
cd /path/to/KV-Raft-Storage/bin
./raftCoreRun -n 3 -f test.conf
```

这条命令的实际行为是：

1. 清空 `test.conf`
2. 主进程循环 `fork()` 出 3 个子进程
3. 每个子进程创建一个 `KvServer`
4. 每个节点启动自己的 RPC 服务
5. 节点把自己的 `ip/port` 追加写入你通过 `-f` 指定的配置文件
6. 所有节点再回头读取这个配置文件，建立互联

### 7.2 启动客户端

新开一个终端，也进入 `bin/`，然后启动客户端：

```bash
cd /path/to/KV-Raft-Storage/bin
./callerMain -f test.conf
```

如果你服务端不是写到 `test.conf`，那客户端也必须读取同一个文件名。例如：

```bash
./raftCoreRun -n 3 -f cluster.conf
./callerMain -f cluster.conf
```

这一点现在已经和代码行为一致：RPC 服务端会把节点地址写入 `-f` 指定的文件，而不是强行写死到 `test.conf`。

## 8. 客户端参数

`callerMain` 当前支持这些参数：

```bash
./callerMain [options]

  -c <count>      请求次数，默认 500
  -o <operation>  操作模式：both / put / get / ttl
  -k <key>        操作的 key，默认 x
  -t <ttl_ms>     ttl 模式下的过期时间，默认 3000
  -f <conf_file>  配置文件，默认 test.conf
  -h              打印帮助
```

示例：

```bash
# 混合读写
./callerMain -c 1000 -o both -k x -f test.conf

# 纯写
./callerMain -c 1000 -o put -k x -f test.conf

# 纯读
./callerMain -c 1000 -o get -k x -f test.conf

# TTL 写入
./callerMain -c 100 -o ttl -k session -t 5000 -f test.conf
```

## 9. 测试方式

当前仓库已经接入的 CTest 目标主要是存储层测试：

```bash
cd /path/to/KV-Raft-Storage/build
ctest --output-on-failure
```

这会跑：

- `storage_cache_test`
- `storage_lsm_test`

如果你只想单独执行它们，也可以直接到 `bin/` 里运行对应可执行文件。

## 10. 当前运行注意事项

### 10.1 必须在 Linux 环境运行

原生 Windows 不是这个工程的目标环境。即便部分头文件能通过，`fork/epoll/ucontext/Muduo` 这条运行链路也不是 Windows 方案。

### 10.2 服务端和客户端要共用同一个配置文件

服务端通过 `-f` 指定写入哪个配置文件，客户端也必须读取同一个文件。

### 10.3 建议从 `bin/` 目录启动

因为配置文件路径是相对当前工作目录处理的。从 `bin/` 启动最直接，服务端和客户端也更容易共用同一份配置。

### 10.4 TTL 后台主动清理默认关闭

这是有意为之。后台线程如果直接删 key，会绕过 Raft 日志复制，破坏状态机确定性。当前默认只保留读时惰性过期。

### 10.5 LSM 不是默认路径

虽然仓库里已经有 `LSMTreeEngine`，但默认还是 `SkipListEngine + LRU`。要体验 LSM，需要构建时显式打开：

```bash
cmake .. -DENABLE_LSM_TREE=ON
```

## 11. 当前代码检查结论

基于当前仓库代码，结论可以分成两层：

### 已确认的部分

- 存储层新增抽象已经接入：`KVEngine / SkipListEngine / CachedKVEngine / LSMTreeEngine`
- `Append` 语义已经是“读取旧值后追加”，不再是覆盖写
- TTL 快照已经进入 `KvServer` 序列化
- TTL 后台主动删除默认关闭
- LRU 和 LSM 都有独立测试目标

### 需要你注意的现实边界

- 当前仓库的自动化测试主要覆盖存储层，不等于“整个 Raft 集群所有路径都已被自动化验证”
- 全量集群运行依赖 Linux 上的 Muduo / Protobuf / Boost / POSIX 环境
- 如果你修改 `.proto`、Muduo 配置、网络线程模型，应该重新做完整集群联调

## 12. 建议的最小验收流程

如果你想确认“这个项目在你的机器上能不能跑”，建议按下面的顺序验收：

```bash
# 1. 编译
cd /path/to/KV-Raft-Storage
mkdir -p build && cd build
cmake ..
make -j"$(nproc)"

# 2. 跑存储层测试
ctest --output-on-failure

# 3. 起 3 节点集群
cd ../bin
./raftCoreRun -n 3 -f test.conf

# 4. 新开终端压测客户端
cd /path/to/KV-Raft-Storage/bin
./callerMain -c 1000 -o both -k x -f test.conf
```

如果这四步都通过，说明你当前机器上的基础编译链、依赖和集群运行链路是通的。
