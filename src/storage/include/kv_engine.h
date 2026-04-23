#ifndef KV_RAFT_STORAGE_KV_ENGINE_H
#define KV_RAFT_STORAGE_KV_ENGINE_H

#include <string>

class KVEngine {
 public:
  virtual ~KVEngine() = default;

  virtual bool Get(const std::string& key, std::string* value) = 0;
  virtual void Put(const std::string& key, const std::string& value) = 0;
  virtual void Delete(const std::string& key) = 0;
  virtual std::string Snapshot() = 0;
  virtual void Restore(const std::string& snapshot) = 0;
  virtual int Size() const = 0;
  virtual void Display() const = 0;
};

#endif  // KV_RAFT_STORAGE_KV_ENGINE_H
