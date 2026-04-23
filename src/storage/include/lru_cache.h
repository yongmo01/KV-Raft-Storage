#ifndef KV_RAFT_STORAGE_LRU_CACHE_H
#define KV_RAFT_STORAGE_LRU_CACHE_H

#include <cstddef>
#include <iterator>
#include <list>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>

class LRUCache {
 public:
  explicit LRUCache(std::size_t capacity) : m_capacity(capacity) {}

  bool Get(const std::string& key, std::string* value) {
    if (m_capacity == 0) {
      return false;
    }

    std::lock_guard<std::mutex> lock(m_mtx);
    auto it = m_index.find(key);
    if (it == m_index.end()) {
      return false;
    }

    m_items.splice(m_items.begin(), m_items, it->second);
    *value = it->second->second;
    return true;
  }

  void Put(const std::string& key, const std::string& value) {
    if (m_capacity == 0) {
      return;
    }

    std::lock_guard<std::mutex> lock(m_mtx);
    auto it = m_index.find(key);
    if (it != m_index.end()) {
      it->second->second = value;
      m_items.splice(m_items.begin(), m_items, it->second);
      return;
    }

    m_items.emplace_front(key, value);
    m_index[key] = m_items.begin();

    if (m_items.size() > m_capacity) {
      auto last = std::prev(m_items.end());
      m_index.erase(last->first);
      m_items.pop_back();
    }
  }

  void Delete(const std::string& key) {
    std::lock_guard<std::mutex> lock(m_mtx);
    auto it = m_index.find(key);
    if (it == m_index.end()) {
      return;
    }

    m_items.erase(it->second);
    m_index.erase(it);
  }

  void Clear() {
    std::lock_guard<std::mutex> lock(m_mtx);
    m_items.clear();
    m_index.clear();
  }

  std::size_t Size() const {
    std::lock_guard<std::mutex> lock(m_mtx);
    return m_items.size();
  }

 private:
  using ItemList = std::list<std::pair<std::string, std::string>>;

  std::size_t m_capacity;
  mutable std::mutex m_mtx;
  ItemList m_items;
  std::unordered_map<std::string, ItemList::iterator> m_index;
};

#endif  // KV_RAFT_STORAGE_LRU_CACHE_H
