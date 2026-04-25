/**
 * @file threadPool.h
 * @brief 轻量级线程池实现
 *
 * 【扩展三】线程池模块
 * 用于替代每次RPC请求都创建新线程的方式，通过复用线程来降低系统开销。
 * 在2核2GB的服务器上尤为重要——避免频繁的线程创建/销毁导致的性能抖动。
 *
 * 设计要点：
 * 1. 基于生产者-消费者模型，使用条件变量进行线程同步
 * 2. 支持任意可调用对象（lambda、function、bind等）
 * 3. 线程安全，支持优雅关闭
 * 4. 通过 ENABLE_THREAD_POOL 宏控制是否启用
 */

#ifndef THREAD_POOL_H
#define THREAD_POOL_H

#include <condition_variable>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>
#include "config.h"

class ThreadPool {
 public:
  /**
   * @brief 构造线程池并启动工作线程
   * @param numThreads 线程数量，默认使用 config.h 中的 RPC_THREAD_POOL_SIZE
   */
  explicit ThreadPool(size_t numThreads = RPC_THREAD_POOL_SIZE) : m_stop(false) {
    for (size_t i = 0; i < numThreads; ++i) {
      m_workers.emplace_back([this]() {
        while (true) {
          std::function<void()> task;
          {
            std::unique_lock<std::mutex> lock(m_mutex);
            // 等待直到有任务可执行或线程池被关闭
            m_cond.wait(lock, [this]() { return m_stop || !m_tasks.empty(); });
            // 线程池关闭且任务队列为空时退出
            if (m_stop && m_tasks.empty()) {
              return;
            }
            task = std::move(m_tasks.front());
            m_tasks.pop();
          }
          // 在锁外执行任务，不阻塞其他任务的提交和调度
          task();
        }
      });
    }
  }

  /**
   * @brief 析构函数，优雅关闭线程池
   * 通知所有线程停止，并等待它们完成当前任务
   */
  ~ThreadPool() {
    {
      std::lock_guard<std::mutex> lock(m_mutex);
      m_stop = true;
    }
    m_cond.notify_all();
    for (auto& worker : m_workers) {
      if (worker.joinable()) {
        worker.join();
      }
    }
  }

  /**
   * @brief 向线程池提交一个任务
   * @tparam F 可调用类型（lambda、std::function、函数指针等）
   * @param task 要执行的任务
   *
   * 使用示例：
   *   pool.enqueue([](){ doSomething(); });
   *   pool.enqueue(std::bind(&MyClass::method, this, arg1));
   */
  template <class F>
  void enqueue(F&& task) {
    {
      std::lock_guard<std::mutex> lock(m_mutex);
      if (m_stop) {
        return;  // 线程池已关闭，不再接受新任务
      }
      m_tasks.emplace(std::forward<F>(task));
    }
    m_cond.notify_one();
  }

  /**
   * @brief 获取当前待处理的任务数量
   * @return 任务队列中等待执行的任务数
   */
  size_t pendingTasks() const {
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_tasks.size();
  }

  // 禁止拷贝和赋值
  ThreadPool(const ThreadPool&) = delete;
  ThreadPool& operator=(const ThreadPool&) = delete;

 private:
  std::vector<std::thread> m_workers;           // 工作线程数组
  std::queue<std::function<void()>> m_tasks;    // 任务队列
  mutable std::mutex m_mutex;                   // 保护任务队列的互斥锁
  std::condition_variable m_cond;               // 用于通知工作线程的条件变量
  bool m_stop;                                  // 线程池停止标志
};

#endif  // THREAD_POOL_H
