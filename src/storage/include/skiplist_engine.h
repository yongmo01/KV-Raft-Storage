#ifndef KV_RAFT_STORAGE_SKIPLIST_ENGINE_H
#define KV_RAFT_STORAGE_SKIPLIST_ENGINE_H

#include <mutex>
#include <shared_mutex>
#include <string>

#include "kv_engine.h"
#include "skipList.h"

class SkipListEngine : public KVEngine {
 public:
  explicit SkipListEngine(int maxLevel = 6) : m_skipList(maxLevel) {}

  bool Get(const std::string& key, std::string* value) override {
    std::shared_lock<std::shared_mutex> lock(m_mtx);
    return m_skipList.search_element(key, *value);
  }

  void Put(const std::string& key, const std::string& value) override {
    std::unique_lock<std::shared_mutex> lock(m_mtx);
    auto mutableKey = key;
    auto mutableValue = value;
    m_skipList.insert_set_element(mutableKey, mutableValue);
  }

  void Delete(const std::string& key) override {
    std::unique_lock<std::shared_mutex> lock(m_mtx);
    m_skipList.delete_element(key);
  }

  std::string Snapshot() override {
    std::shared_lock<std::shared_mutex> lock(m_mtx);
    return m_skipList.dump_file();
  }

  void Restore(const std::string& snapshot) override {
    std::unique_lock<std::shared_mutex> lock(m_mtx);
    m_skipList.load_file(snapshot);
  }

  int Size() const override {
    std::shared_lock<std::shared_mutex> lock(m_mtx);
    return m_skipList.size();
  }

  void Display() const override {
    std::shared_lock<std::shared_mutex> lock(m_mtx);
    m_skipList.display_list();
  }

 private:
  mutable std::shared_mutex m_mtx;
  mutable SkipList<std::string, std::string> m_skipList;
};

#endif  // KV_RAFT_STORAGE_SKIPLIST_ENGINE_H
