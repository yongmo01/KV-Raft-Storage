# KV-Raft-Storage 项目上手指南

这是一份为你量身定制的专属 README，帮助你快速理解、编译和运行在这个远端 Linux/Ubuntu 服务器上的 KV-Raft-Storage 项目。

---

## 🚀 1. 项目概览

**KV-Raft-Storage** 是一个基于 **Raft 一致性算法**的分布式键值数据库。
为了追求极致的性能与高并发，项目不仅仅实现了 Raft，还从零构建了底层的**协程库**和**RPC框架**，并使用**跳表（SkipList）**作为内存存储引擎。

主要技术栈/核心组件：
- **Raft 算法**：实现集群Leader选举、日志复制、持久化与快照（MIT 6.824 规范）。
- **Fiber 协程库**：基于 ucontext，包含协程调度、IO 协程池以及针对 Socket 通信的 Hook 技术。
- **MPRPC 框架**：基于 Protobuf 与 Fiber 协程的高性能 RPC 框架。
- **SkipList**：高效的底层并发内存状态机存储结构。

---

## 📂 2. 项目结构与模块解析

项目高度模块化，核心代码集中在 `src/` 目录下。

```text
KV-Raft-Storage/
├── src/                # 🌟 核心源码目录
│   ├── fiber/          # 【协程模块】实现用户态线程调度、IO管理器、定时器及系统API Hook
│   ├── rpc/            # 【RPC模块】基于Protobuf搭建的RPC网络通讯框架
│   ├── raftRpcPro/     # 【协议模块】Raft与KV Server交互的Protobuf协议文件(.proto及生成代码)
│   ├── skipList/       # 【存储模块】线程安全的跳跃表，作为KV状态机底层的内存引擎
│   ├── raftCore/       # 【Raft核心核心】核心算法层：raft.cpp(共识机制)、kvServer.cpp(KV逻辑)、Persister(持久化)
│   ├── raftClerk/      # 【客户端模块】RPC客户端（Clerk），用于给Raft集群发送请求（Put/Append/Get）
│   └── common/         # 【公共模块】通用的工具类与配置项
├── example/            # 🏃‍♂️ 各个独立模块的测试用例（协程、RPC、Raft的隔离测试）
├── test/               # 🧪 整体集成测试文件
├── docs/               # 📚 相关文档库
├── bin/                # 📦 编译产物目录（可执行文件和配置文件 test.conf 将被置于此）
├── CMakeLists.txt      # ⚙️ 顶层 CMake 构建脚本
└── format.sh           # 🧹 格式化代码脚本
```

### 🧩 模块间关联关系：
1. **网络与并发基础**：`fiber` 管理所有并行任务。`rpc` 层利用 `fiber` 实现高并发的非阻塞 RPC 调用。
2. **共识与通讯**：`raftCore` 节点之间通过 `rpc` 和 `raftRpcPro` 中的协议规范交换心跳和日志。
3. **服务与存储**：外部请求由 `raftClerk` 发起，`raftCore` 接收并在集群中达成一致后，应用到 `skipList` 存储引擎中。

---

## 🛠️ 3. 编译指南 (Linux / Ubuntu)

该项目采用 **CMake** 构建。请确保你的 Linux 机器上安装了 `g++`、`cmake` 和 `protobuf` 包。

### 常规编译命令
```bash
# 1. 确保在项目根目录
cd /path/to/KV-Raft-Storage

# 2. 创建并进入独立的构建目录 (Out-of-source build)
mkdir build && cd build

# 3. 生成 Makefile 并执行编译 (-j 指定多核编译，提升速度)
cmake ..
make -j4
```

### 预期结果
编译成功后，终端将输出 `[100%] Built target ...` 等字样。 
编译生成的可执行二进制文件（比如各种 tests 与 examples）将被自动放置在项目的 `bin/` 目录下。

---

## ▶️ 4. 运行说明与预期

项目编译完成后，通常需要进入 `bin/` 目录以读取默认的配置文件（如 `test.conf`）。

### 运行集群测试 / 示例
```bash
# 1. 切换到可执行文件目录
cd ../bin

# 2. 查看或修改配置档 (可选)
# cat test.conf 

# 3. 运行任意一个 Raft 或 RPC 测试用例，例如：
./raftCoreExample   # 具体名字视目标构建产物而定
```

### 预期结果
- **RPC 通信**：终端应当打印出 Provider（服务端）注册的服务以及消费者调用 RPC 接口的成功日志。
- **Raft 集群**：终端将循环打印节点状态（Leader、Follower、Candidate），你能观察到 `heartbeat`（心跳包）、`election timeout`（选举超时触发投票）以及日志复制的过程。如果有节点断开，其余节点会自动进行换届选举。

---

## 💡 5. 扩展与修改建议 (For You)

这份代码结构非常清晰，当你想进行二次开发时，可以按照以下思路：

1. **想改网络/并发性能？** 👉 到 `src/fiber` 里面优化调度算法或者 epoll 逻辑。
2. **想新增 RPC 接口？** 👉 
   - 第一步：在 `src/raftRpcPro/` 添加 `.proto` 定义，使用 protoc 生成代码。
   - 第二步：在 `src/rpc/` 注册新服务，并在 `src/raftCore` 实现具体逻辑。
3. **想优化存储引擎？** 👉 到 `src/skipList/` 替换或改进当前的跳表实现（比如尝试引入 RocksDB/LevelDB 作为磁盘落地引擎）。
4. **想改写业务逻辑？** 👉 `src/raftCore/kvServer.cpp` 是暴露给客户端的业务入口，处理键值的 `Put/Get/Append` 逻辑。

> **🌟 小贴士**：开发时请尽量保持每个模块相互解耦，编写完成后可以在 `example/` 目录下添加一个小范围的测试，通过后再整合进系统测试。
