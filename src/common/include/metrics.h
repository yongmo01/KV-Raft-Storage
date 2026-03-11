/**
 * @file metrics.h
 * @brief 运行时指标监控系统
 *
 * 【扩展四】Metrics 监控模块
 * 在分布式KV存储的关键路径上进行埋点，收集延迟、吞吐量、Raft状态等运行时指标。
 * 这是企业级分布式系统的标配，体现对"可观测性（Observability）"的重视。
 *
 * 设计要点：
 * 1. 单例模式，全局唯一实例
 * 2. 线程安全（所有操作加锁保护）
 * 3. 支持三种指标类型：Counter（计数器）、Gauge（仪表盘）、Latency（延迟直方图）
 * 4. 提供文本格式输出（类Prometheus格式），便于后续对接监控系统
 * 5. 通过 ENABLE_METRICS 宏控制是否启用，关闭后所有埋点宏为空操作，零开销
 */

#ifndef METRICS_H
#define METRICS_H

#include <algorithm>
#include <atomic>
#include <chrono>
#include <mutex>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>
#include "config.h"

#if ENABLE_METRICS

class Metrics {
 public:
  /**
   * @brief 获取Metrics单例
   * 使用C++11的static局部变量保证线程安全的懒初始化
   */
  static Metrics& getInstance() {
    static Metrics instance;
    return instance;
  }

  /**
   * @brief 计数器+1
   * @param name 指标名称，例如 "kv_get_total", "raft_election_total"
   *
   * 适用于：请求次数、选举次数、快照次数等单调递增的指标
   */
  void incCounter(const std::string& name) {
    std::lock_guard<std::mutex> lg(m_mtx);
    m_counters[name]++;
  }

  /**
   * @brief 设置仪表值（可增可减的瞬时值）
   * @param name 指标名称，例如 "raft_commit_index", "raft_term"
   * @param val 当前值
   *
   * 适用于：当前term、commitIndex、日志大小等实时状态值
   */
  void setGauge(const std::string& name, int64_t val) {
    std::lock_guard<std::mutex> lg(m_mtx);
    m_gauges[name] = val;
  }

  /**
   * @brief 记录延迟数据点（微秒）
   * @param name 指标名称，例如 "kv_get_latency_us"
   * @param us 延迟值（微秒）
   *
   * 保存最近的数据点用于计算p50/p99分位数。
   * 为了控制内存，每个指标最多保留最近10000个数据点。
   */
  void recordLatency(const std::string& name, int64_t us) {
    std::lock_guard<std::mutex> lg(m_mtx);
    auto& vec = m_latencies[name];
    if (vec.size() >= 10000) {
      // 滑动窗口：丢弃最老的一半数据，保证内存可控
      vec.erase(vec.begin(), vec.begin() + vec.size() / 2);
    }
    vec.push_back(us);
  }

  /**
   * @brief 输出所有指标的文本表示（类Prometheus格式）
   * @return 格式化的指标字符串
   *
   * 输出示例：
   *   # COUNTERS
   *   kv_get_total 1234
   *   # GAUGES
   *   raft_term 5
   *   # LATENCIES (us)
   *   kv_get_latency_us_p50 120
   *   kv_get_latency_us_p99 890
   */
  std::string dump() {
    std::lock_guard<std::mutex> lg(m_mtx);
    std::ostringstream oss;
    oss << "# ======================== METRICS DUMP ========================\n";

    // 输出计数器
    oss << "# COUNTERS\n";
    for (const auto& kv : m_counters) {
      oss << kv.first << " " << kv.second << "\n";
    }

    // 输出仪表值
    oss << "# GAUGES\n";
    for (const auto& kv : m_gauges) {
      oss << kv.first << " " << kv.second << "\n";
    }

    // 输出延迟分位数
    oss << "# LATENCIES (us)\n";
    for (auto& kv : m_latencies) {
      if (kv.second.empty()) continue;
      // 排序用于计算分位数
      std::vector<int64_t> sorted = kv.second;
      std::sort(sorted.begin(), sorted.end());
      size_t n = sorted.size();
      oss << kv.first << "_count " << n << "\n";
      oss << kv.first << "_p50 " << sorted[n * 50 / 100] << "\n";
      oss << kv.first << "_p99 " << sorted[std::min(n * 99 / 100, n - 1)] << "\n";
      oss << kv.first << "_max " << sorted[n - 1] << "\n";
    }
    oss << "# ==============================================================\n";
    return oss.str();
  }

  /**
   * @brief 重置所有指标（主要用于测试）
   */
  void reset() {
    std::lock_guard<std::mutex> lg(m_mtx);
    m_counters.clear();
    m_gauges.clear();
    m_latencies.clear();
  }

 private:
  Metrics() = default;
  Metrics(const Metrics&) = delete;
  Metrics& operator=(const Metrics&) = delete;

  std::mutex m_mtx;
  std::unordered_map<std::string, int64_t> m_counters;          // 计数器
  std::unordered_map<std::string, int64_t> m_gauges;            // 仪表值
  std::unordered_map<std::string, std::vector<int64_t>> m_latencies;  // 延迟直方图
};

// ====================== 便捷埋点宏 ======================
// 使用宏封装，当ENABLE_METRICS关闭时所有埋点变成空操作，零开销

#define METRICS_INC_COUNTER(name) Metrics::getInstance().incCounter(name)
#define METRICS_SET_GAUGE(name, val) Metrics::getInstance().setGauge(name, val)
#define METRICS_RECORD_LATENCY(name, us) Metrics::getInstance().recordLatency(name, us)
#define METRICS_DUMP() Metrics::getInstance().dump()

/**
 * @brief RAII风格的延迟计时器，用于自动记录函数/代码块的执行耗时
 *
 * 使用示例：
 *   void Get() {
 *       METRICS_TIMER("kv_get_latency_us");
 *       // ... 业务逻辑 ...
 *   }  // 离开作用域时自动记录耗时
 */
class MetricsTimer {
 public:
  MetricsTimer(const std::string& name) : m_name(name), m_start(std::chrono::high_resolution_clock::now()) {}
  ~MetricsTimer() {
    auto end = std::chrono::high_resolution_clock::now();
    auto us = std::chrono::duration_cast<std::chrono::microseconds>(end - m_start).count();
    Metrics::getInstance().recordLatency(m_name, us);
  }

 private:
  std::string m_name;
  std::chrono::high_resolution_clock::time_point m_start;
};

#define METRICS_TIMER(name) MetricsTimer _metrics_timer_##__LINE__(name)

#else  // ENABLE_METRICS == 0

// 当 Metrics 关闭时，所有宏展开为空操作
#define METRICS_INC_COUNTER(name) ((void)0)
#define METRICS_SET_GAUGE(name, val) ((void)0)
#define METRICS_RECORD_LATENCY(name, us) ((void)0)
#define METRICS_DUMP() std::string("")
#define METRICS_TIMER(name) ((void)0)

#endif  // ENABLE_METRICS

#endif  // METRICS_H
