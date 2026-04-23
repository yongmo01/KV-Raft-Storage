#ifndef KV_RAFT_STORAGE_CACHED_KV_ENGINE_H
#define KV_RAFT_STORAGE_CACHED_KV_ENGINE_H

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include "kv_engine.h"
#include "lru_cache.h"
#include "metrics.h"

class CachedKVEngine : public KVEngine {
 public:
  CachedKVEngine(std::unique_ptr<KVEngine> base, std::size_t cacheCapacity)
      : m_base(std::move(base)), m_cache(cacheCapacity) {}

  bool Get(const std::string& key, std::string* value) override {
    if (m_cache.Get(key, value)) {
      METRICS_INC_COUNTER("cache_hit_total");
      return true;
    }

    METRICS_INC_COUNTER("cache_miss_total");
    if (!m_base->Get(key, value)) {
      return false;
    }

    m_cache.Put(key, *value);
    METRICS_SET_GAUGE("cache_size", static_cast<int64_t>(m_cache.Size()));
    return true;
  }

  void Put(const std::string& key, const std::string& value) override {
    m_base->Put(key, value);
    m_cache.Put(key, value);
    METRICS_SET_GAUGE("cache_size", static_cast<int64_t>(m_cache.Size()));
  }

  void Delete(const std::string& key) override {
    m_base->Delete(key);
    m_cache.Delete(key);
    METRICS_SET_GAUGE("cache_size", static_cast<int64_t>(m_cache.Size()));
  }

  std::string Snapshot() override { return m_base->Snapshot(); }

  void Restore(const std::string& snapshot) override {
    m_base->Restore(snapshot);
    m_cache.Clear();
    METRICS_SET_GAUGE("cache_size", 0);
  }

  int Size() const override { return m_base->Size(); }

  void Display() const override { m_base->Display(); }

 private:
  std::unique_ptr<KVEngine> m_base;
  LRUCache m_cache;
};

#endif  // KV_RAFT_STORAGE_CACHED_KV_ENGINE_H
