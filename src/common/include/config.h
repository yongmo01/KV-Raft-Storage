//
// Created by swx on 23-12-23.
//

#ifndef CONFIG_H
#define CONFIG_H

const bool Debug = true;

const int debugMul = 1;  // 时间单位：time.Millisecond，不同网络环境rpc速度不同，因此需要乘以一个系数
const int HeartBeatTimeout = 25 * debugMul;  // 心跳时间一般要比选举超时小一个数量级
const int ApplyInterval = 10 * debugMul;     //

const int minRandomizedElectionTime = 300 * debugMul;  // ms
const int maxRandomizedElectionTime = 500 * debugMul;  // ms

const int CONSENSUS_TIMEOUT = 500 * debugMul;  // ms

// ====================== 扩展功能开关（编译期开关） ======================
// 每个扩展功能通过宏定义的方式进行开关控制，编译时可通过cmake的-D选项覆盖。
// 例如：cmake -DENABLE_READ_INDEX=OFF .. 可关闭 ReadIndex 读优化
// 如果不传参数，则使用下面的默认值。

// 【扩展一】ReadIndex 读优化开关
// 开启后，Get请求在Leader上不走日志复制流程，而是通过确认Leadership后直接读取状态机，大幅降低读延迟。
// 关闭后，Get请求走原有的日志复制流程（Start -> waitApply）。
#ifndef ENABLE_READ_INDEX
#define ENABLE_READ_INDEX 1
#endif

// 【扩展二】Key TTL 过期淘汰机制开关
// 开启后，PutAppend支持携带TTL参数，过期的key会被惰性删除（读时检查）和定期清理（后台线程扫描）。
// 关闭后，行为与原项目一致，所有key永不过期。
#ifndef ENABLE_KEY_TTL
#define ENABLE_KEY_TTL 1
#endif

// 【扩展三】线程池开关
// 开启后，RPC业务处理使用线程池，避免为每个请求创建新线程。
// 关闭后，行为与原项目一致。
#ifndef ENABLE_THREAD_POOL
#define ENABLE_THREAD_POOL 1
#endif

// 【扩展四】Metrics 运行时监控开关
// 开启后，在关键路径（Get/Put/Append/选举/心跳）埋点记录延迟、计数等指标，
// 并通过简单的文本接口输出，用于运行时观测系统状态。
// 关闭后，无任何监控开销。
#ifndef ENABLE_METRICS
#define ENABLE_METRICS 1
#endif

// ====================== 扩展功能参数配置 ======================
// TTL 定期清理间隔（毫秒），每隔该时间后台线程扫描一次过期key
const int TTL_CLEANUP_INTERVAL_MS = 100;
// TTL 每次定期清理最多扫描的key数量
const int TTL_CLEANUP_SAMPLE_COUNT = 20;

// 线程池默认线程数量（适配2核CPU服务器）
const int THREAD_POOL_SIZE = 4;

// Metrics 输出间隔（毫秒），定期将指标打印到日志
const int METRICS_DUMP_INTERVAL_MS = 10000;

// 协程相关设置

const int FIBER_THREAD_NUM = 1;              // 协程库中线程池大小
const bool FIBER_USE_CALLER_THREAD = false;  // 是否使用caller_thread执行调度任务

#endif  // CONFIG_H
