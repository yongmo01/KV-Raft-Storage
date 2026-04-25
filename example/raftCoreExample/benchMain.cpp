#include <getopt.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "clerk.h"
#include "config.h"

struct BenchOptions {
  long long count = 10000;
  int threads = 1;
  std::string op = "put";
  std::string key = "bench";
  std::string prefix = "bench";
  std::string keyMode = "hot";
  int valueSize = 128;
  int readRatio = 50;
  int ttlMs = 3000;
  int warmup = 0;
  int keyRange = 1024;
  int rateLimit = 0;
  std::string confFile = "test.conf";
  bool json = false;
};

struct BenchResult {
  long long total = 0;
  long long success = 0;
  long long failed = 0;
  long long elapsedMs = 0;
  double qps = 0.0;
  double avgUs = 0.0;
  long long p50Us = 0;
  long long p95Us = 0;
  long long p99Us = 0;
};

void PrintHelp() {
  std::cout << "Usage: ./benchMain [options]\n"
            << "Options:\n"
            << "  -c <count>       总请求数，默认 10000\n"
            << "  -j <threads>     客户端并发线程数，默认 1\n"
            << "  -o <operation>   操作类型：put/get/append/both/ttl，默认 put\n"
            << "  -k <key>         hot 模式下使用的 key，默认 bench\n"
            << "  -p <prefix>      unique/range 模式下使用的 key 前缀，默认 bench\n"
            << "  -m <mode>        key 分布：hot/unique/range，默认 hot\n"
            << "  -g <range>       range 模式下的 key 数量，默认 1024\n"
            << "  -s <size>        value 大小，单位字节，默认 128\n"
            << "  -r <ratio>       both 模式下读比例，0-100，默认 50\n"
            << "  -t <ttl_ms>      ttl 模式下的 TTL，单位毫秒，默认 3000\n"
            << "  -f <conf_file>   集群配置文件，默认 test.conf\n"
            << "  --warmup <N>     正式统计前的预热请求数，默认 0\n"
            << "  --rate <N>       全局限速，每秒最多发送 N 个请求，默认 0 表示不限速\n"
            << "  --json           使用 JSON 格式输出结果\n"
            << "  -h, --help       打印帮助\n";
}

bool IsValidOperation(const std::string& op) {
  return op == "put" || op == "get" || op == "append" || op == "both" || op == "ttl";
}

bool IsValidKeyMode(const std::string& mode) { return mode == "hot" || mode == "unique" || mode == "range"; }

std::string MakeKey(const BenchOptions& options, int workerId, long long opIndex) {
  if (options.keyMode == "hot") {
    return options.key;
  }
  if (options.keyMode == "unique") {
    return options.prefix + "_" + std::to_string(workerId) + "_" + std::to_string(opIndex);
  }
  return options.prefix + "_" + std::to_string(opIndex % options.keyRange);
}

std::string MakeValue(const BenchOptions& options) {
  if (options.valueSize <= 0) {
    return "";
  }
  return std::string(static_cast<size_t>(options.valueSize), 'x');
}

void RunOneOperation(Clerk& client, const BenchOptions& options, int workerId, long long opIndex,
                     std::mt19937& rng, const std::string& value) {
  const std::string key = MakeKey(options, workerId, opIndex);
  std::string actualOp = options.op;
  if (actualOp == "both") {
    std::uniform_int_distribution<int> dist(1, 100);
    actualOp = (dist(rng) <= options.readRatio) ? "get" : "put";
  }

  if (actualOp == "get") {
    (void)client.Get(key);
  } else if (actualOp == "put") {
    client.Put(key, value);
  } else if (actualOp == "append") {
    client.Append(key, value);
  } else if (actualOp == "ttl") {
#if ENABLE_KEY_TTL
    client.PutWithTTL(key, value, options.ttlMs);
#else
    client.Put(key, value);
#endif
  }
}

long long Percentile(std::vector<long long>& values, double percentile) {
  if (values.empty()) {
    return 0;
  }
  std::sort(values.begin(), values.end());
  const double rank = percentile / 100.0 * static_cast<double>(values.size() - 1);
  return values[static_cast<size_t>(rank)];
}

BenchOptions ParseArgs(int argc, char** argv) {
  BenchOptions options;
  static option longOptions[] = {
      {"warmup", required_argument, nullptr, 1000},
      {"json", no_argument, nullptr, 1001},
      {"rate", required_argument, nullptr, 1002},
      {"help", no_argument, nullptr, 'h'},
      {nullptr, 0, nullptr, 0},
  };

  int opt = 0;
  while ((opt = getopt_long(argc, argv, "c:j:o:k:p:m:g:s:r:t:f:h", longOptions, nullptr)) != -1) {
    switch (opt) {
      case 'c':
        options.count = std::stoll(optarg);
        break;
      case 'j':
        options.threads = std::stoi(optarg);
        break;
      case 'o':
        options.op = optarg;
        break;
      case 'k':
        options.key = optarg;
        break;
      case 'p':
        options.prefix = optarg;
        break;
      case 'm':
        options.keyMode = optarg;
        break;
      case 'g':
        options.keyRange = std::stoi(optarg);
        break;
      case 's':
        options.valueSize = std::stoi(optarg);
        break;
      case 'r':
        options.readRatio = std::stoi(optarg);
        break;
      case 't':
        options.ttlMs = std::stoi(optarg);
        break;
      case 'f':
        options.confFile = optarg;
        break;
      case 1000:
        options.warmup = std::stoi(optarg);
        break;
      case 1001:
        options.json = true;
        break;
      case 1002:
        options.rateLimit = std::stoi(optarg);
        break;
      case 'h':
      default:
        PrintHelp();
        std::exit(0);
    }
  }

  if (options.count < 0) {
    options.count = 0;
  }
  if (options.threads <= 0) {
    options.threads = 1;
  }
  if (options.valueSize < 0) {
    options.valueSize = 0;
  }
  if (options.readRatio < 0) {
    options.readRatio = 0;
  }
  if (options.readRatio > 100) {
    options.readRatio = 100;
  }
  if (options.keyRange <= 0) {
    options.keyRange = 1;
  }
  if (options.rateLimit < 0) {
    options.rateLimit = 0;
  }
  if (!IsValidOperation(options.op)) {
    std::cerr << "不支持的操作类型: " << options.op << std::endl;
    std::exit(1);
  }
  if (!IsValidKeyMode(options.keyMode)) {
    std::cerr << "不支持的 key 分布模式: " << options.keyMode << std::endl;
    std::exit(1);
  }
  return options;
}

void RunWarmup(const BenchOptions& options, const std::string& value) {
  if (options.warmup <= 0) {
    return;
  }
  Clerk client;
  client.Init(options.confFile);
  std::mt19937 rng(1);
  for (int i = 0; i < options.warmup; ++i) {
    RunOneOperation(client, options, 0, i, rng, value);
  }
}

BenchResult RunBenchmark(const BenchOptions& options) {
  const std::string value = MakeValue(options);
  RunWarmup(options, value);

  std::atomic<long long> nextOp{0};
  std::atomic<long long> success{0};
  std::atomic<long long> failed{0};
  std::atomic<int> readyWorkers{0};
  std::atomic<bool> start{false};
  std::vector<std::vector<long long>> threadLatencies(static_cast<size_t>(options.threads));
  std::vector<std::thread> workers;
  workers.reserve(static_cast<size_t>(options.threads));

  std::chrono::steady_clock::time_point begin;
  for (int workerId = 0; workerId < options.threads; ++workerId) {
    workers.emplace_back([&, workerId]() {
      Clerk client;
      client.Init(options.confFile);
      std::mt19937 rng(static_cast<unsigned int>(workerId + 1));
      auto& localLatencies = threadLatencies[static_cast<size_t>(workerId)];
      localLatencies.reserve(static_cast<size_t>(options.count / options.threads + 1));

      readyWorkers.fetch_add(1);
      while (!start.load()) {
        std::this_thread::yield();
      }

      while (true) {
        const long long opIndex = nextOp.fetch_add(1);
        if (opIndex >= options.count) {
          break;
        }
        if (options.rateLimit > 0) {
          const auto targetDelay =
              std::chrono::microseconds((opIndex * 1000000LL) / options.rateLimit);
          std::this_thread::sleep_until(begin + targetDelay);
        }

        const auto opBegin = std::chrono::steady_clock::now();
        try {
          RunOneOperation(client, options, workerId, opIndex, rng, value);
          success.fetch_add(1);
        } catch (...) {
          failed.fetch_add(1);
        }
        const auto opEnd = std::chrono::steady_clock::now();
        localLatencies.push_back(std::chrono::duration_cast<std::chrono::microseconds>(opEnd - opBegin).count());
      }
    });
  }

  while (readyWorkers.load() < options.threads) {
    std::this_thread::yield();
  }
  begin = std::chrono::steady_clock::now();
  start.store(true);

  for (auto& worker : workers) {
    worker.join();
  }
  const auto end = std::chrono::steady_clock::now();

  std::vector<long long> latencies;
  for (const auto& item : threadLatencies) {
    latencies.insert(latencies.end(), item.begin(), item.end());
  }

  BenchResult result;
  result.total = options.count;
  result.success = success.load();
  result.failed = failed.load();
  result.elapsedMs = std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count();
  const double elapsedSeconds = static_cast<double>(result.elapsedMs) / 1000.0;
  result.qps = elapsedSeconds > 0.0 ? static_cast<double>(result.success) / elapsedSeconds : 0.0;

  long long latencySum = 0;
  for (const auto latency : latencies) {
    latencySum += latency;
  }
  result.avgUs = latencies.empty() ? 0.0 : static_cast<double>(latencySum) / static_cast<double>(latencies.size());
  result.p50Us = Percentile(latencies, 50.0);
  result.p95Us = Percentile(latencies, 95.0);
  result.p99Us = Percentile(latencies, 99.0);
  return result;
}

void PrintResult(const BenchOptions& options, const BenchResult& result) {
  if (options.json) {
    std::cout << "{"
              << "\"operation\":\"" << options.op << "\","
              << "\"key_mode\":\"" << options.keyMode << "\","
              << "\"threads\":" << options.threads << ","
              << "\"rate_limit\":" << options.rateLimit << ","
              << "\"total\":" << result.total << ","
              << "\"success\":" << result.success << ","
              << "\"failed\":" << result.failed << ","
              << "\"elapsed_ms\":" << result.elapsedMs << ","
              << "\"qps\":" << std::fixed << std::setprecision(2) << result.qps << ","
              << "\"avg_us\":" << std::fixed << std::setprecision(2) << result.avgUs << ","
              << "\"p50_us\":" << result.p50Us << ","
              << "\"p95_us\":" << result.p95Us << ","
              << "\"p99_us\":" << result.p99Us << "}" << std::endl;
    return;
  }

  std::cout << "========== Benchmark Result ==========\n"
            << "操作类型: " << options.op << "\n"
            << "key 分布: " << options.keyMode << "\n"
            << "并发线程: " << options.threads << "\n"
            << "限速: "
            << (options.rateLimit > 0 ? std::to_string(options.rateLimit) + " req/s" : "不限速") << "\n"
            << "总请求数: " << result.total << "\n"
            << "成功请求: " << result.success << "\n"
            << "失败请求: " << result.failed << "\n"
            << "总耗时: " << result.elapsedMs << " ms\n"
            << "QPS: " << std::fixed << std::setprecision(2) << result.qps << "\n"
            << "平均延迟: " << std::fixed << std::setprecision(2) << result.avgUs << " us\n"
            << "P50: " << result.p50Us << " us\n"
            << "P95: " << result.p95Us << " us\n"
            << "P99: " << result.p99Us << " us\n";
}

int main(int argc, char** argv) {
  const BenchOptions options = ParseArgs(argc, argv);
  const BenchResult result = RunBenchmark(options);
  PrintResult(options, result);
  return result.failed == 0 ? 0 : 1;
}
