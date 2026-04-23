#include "cached_kv_engine.h"
#include "lru_cache.h"

#include <cassert>
#include <memory>
#include <string>
#include <unordered_map>

class MapEngine : public KVEngine {
 public:
  bool Get(const std::string& key, std::string* value) override {
    auto it = data_.find(key);
    if (it == data_.end()) {
      return false;
    }
    *value = it->second;
    return true;
  }

  void Put(const std::string& key, const std::string& value) override { data_[key] = value; }

  void Delete(const std::string& key) override { data_.erase(key); }

  std::string Snapshot() override { return data_["snapshot"]; }

  void Restore(const std::string& snapshot) override {
    data_.clear();
    data_["snapshot"] = snapshot;
  }

  int Size() const override { return static_cast<int>(data_.size()); }

  void Display() const override {}

 private:
  std::unordered_map<std::string, std::string> data_;
};

void TestLRUCache() {
  LRUCache cache(2);
  std::string value;

  assert(!cache.Get("a", &value));
  cache.Put("a", "1");
  cache.Put("b", "2");
  assert(cache.Get("a", &value));
  assert(value == "1");

  cache.Put("c", "3");
  assert(cache.Get("a", &value));
  assert(!cache.Get("b", &value));
  assert(cache.Get("c", &value));
  assert(value == "3");

  cache.Put("a", "10");
  assert(cache.Get("a", &value));
  assert(value == "10");

  cache.Delete("a");
  assert(!cache.Get("a", &value));
}

void TestCachedKVEngine() {
  auto base = std::make_unique<MapEngine>();
  CachedKVEngine engine(std::move(base), 2);
  std::string value;

  engine.Put("a", "1");
  assert(engine.Get("a", &value));
  assert(value == "1");

  engine.Put("a", "2");
  assert(engine.Get("a", &value));
  assert(value == "2");

  engine.Delete("a");
  assert(!engine.Get("a", &value));

  engine.Put("snapshot", "persisted");
  assert(engine.Snapshot() == "persisted");
  engine.Put("snapshot", "cached");
  engine.Restore("restored");
  assert(engine.Get("snapshot", &value));
  assert(value == "restored");
}

int main() {
  TestLRUCache();
  TestCachedKVEngine();
  return 0;
}
