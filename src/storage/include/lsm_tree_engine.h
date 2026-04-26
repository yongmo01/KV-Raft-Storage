#ifndef KV_RAFT_STORAGE_LSM_TREE_ENGINE_H
#define KV_RAFT_STORAGE_LSM_TREE_ENGINE_H

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <fstream>
#include <functional>
#include <iostream>
#include <map>
#include <mutex>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>

#include "kv_engine.h"

class LSMTreeEngine : public KVEngine {
 public:
  explicit LSMTreeEngine(std::size_t memTableFlushThreshold = 1024)
      : m_memTableFlushThreshold(memTableFlushThreshold == 0 ? 1 : memTableFlushThreshold) {}

  LSMTreeEngine(std::size_t memTableFlushThreshold, std::string walPath)
      : m_memTableFlushThreshold(memTableFlushThreshold == 0 ? 1 : memTableFlushThreshold),
        m_walPath(std::move(walPath)) {
    RecoverFromWALLocked();
  }

  LSMTreeEngine(std::size_t memTableFlushThreshold, std::string walPath, std::string manifestPath)
      : m_memTableFlushThreshold(memTableFlushThreshold == 0 ? 1 : memTableFlushThreshold),
        m_walPath(std::move(walPath)),
        m_manifestPath(std::move(manifestPath)) {
    RecoverFromManifestLocked();
    RecoverFromWALLocked();
  }

  ~LSMTreeEngine() override { StopBackgroundWorker(); }

  bool Get(const std::string& key, std::string* value) override {
    std::shared_lock<std::shared_mutex> lock(m_mtx);
    Entry entry;
    if (FindEntryLocked(key, &entry)) {
      if (entry.type == EntryType::Delete) {
        return false;
      }
      *value = entry.value;
      return true;
    }
    return false;
  }

  void Put(const std::string& key, const std::string& value) override {
    std::unique_lock<std::shared_mutex> lock(m_mtx);
    AppendWALLocked(EntryType::Put, key, value);
    m_memTable[key] = Entry{EntryType::Put, value};
    FlushMemTableIfNeededLocked();
  }

  void Delete(const std::string& key) override {
    std::unique_lock<std::shared_mutex> lock(m_mtx);
    AppendWALLocked(EntryType::Delete, key, "");
    m_memTable[key] = Entry{EntryType::Delete, ""};
    FlushMemTableIfNeededLocked();
  }

  std::string Snapshot() override {
    std::shared_lock<std::shared_mutex> lock(m_mtx);
    auto data = ExportVisibleDataLocked();
    std::string snapshot;
    WriteUint64(&snapshot, static_cast<uint64_t>(data.size()));
    for (const auto& item : data) {
      WriteString(&snapshot, item.first);
      WriteString(&snapshot, item.second);
    }
    return snapshot;
  }

  void Restore(const std::string& snapshot) override {
    std::map<std::string, Entry> restored;
    if (snapshot.empty()) {
      std::unique_lock<std::shared_mutex> lock(m_mtx);
      DeleteSSTableFilesLocked();
      m_memTable.clear();
      m_immutableMemTables.clear();
      m_sstables.clear();
      RewriteWALLocked();
      RewriteManifestLocked();
      return;
    }

    std::size_t offset = 0;
    uint64_t count = ReadUint64(snapshot, &offset);
    for (uint64_t i = 0; i < count; ++i) {
      std::string key = ReadString(snapshot, &offset);
      std::string value = ReadString(snapshot, &offset);
      restored[key] = Entry{EntryType::Put, value};
    }
    if (offset != snapshot.size()) {
      throw std::runtime_error("LSM snapshot has trailing bytes");
    }

    std::unique_lock<std::shared_mutex> lock(m_mtx);
    DeleteSSTableFilesLocked();
    m_memTable = std::move(restored);
    m_immutableMemTables.clear();
    m_sstables.clear();
    RewriteWALLocked();
    RewriteManifestLocked();
    FlushMemTableIfNeededLocked();
  }

  int Size() const override {
    std::shared_lock<std::shared_mutex> lock(m_mtx);
    return static_cast<int>(ExportVisibleDataLocked().size());
  }

  void Display() const override {
    std::shared_lock<std::shared_mutex> lock(m_mtx);
    for (const auto& item : ExportVisibleDataLocked()) {
      std::cout << item.first << ":" << item.second << std::endl;
    }
  }

  void Flush() {
    std::unique_lock<std::shared_mutex> lock(m_mtx);
    FlushMemTableLocked();
  }

  void Compact() {
    std::unique_lock<std::shared_mutex> lock(m_mtx);
    CompactSSTablesLocked();
  }

  std::size_t SSTableCount() const {
    std::shared_lock<std::shared_mutex> lock(m_mtx);
    return m_sstables.size();
  }

  std::vector<std::pair<std::string, std::string>> RangeScan(const std::string& startKey,
                                                             const std::string& endKey) const {
    std::shared_lock<std::shared_mutex> lock(m_mtx);
    std::vector<std::pair<std::string, std::string>> result;
    if (startKey > endKey) {
      return result;
    }

    auto data = ExportVisibleDataLocked();
    auto it = data.lower_bound(startKey);
    while (it != data.end() && it->first <= endKey) {
      result.emplace_back(it->first, it->second);
      ++it;
    }
    return result;
  }

  void StartBackgroundWorker(int intervalMs = 1000) {
    std::lock_guard<std::mutex> lock(m_workerMtx);
    if (m_backgroundRunning) {
      return;
    }
    m_backgroundStop = false;
    m_backgroundRunning = true;
    m_backgroundWorker = std::thread(&LSMTreeEngine::BackgroundWorkerLoop, this, intervalMs <= 0 ? 1000 : intervalMs);
  }

  void EnableAsyncFlush(bool enabled = true) {
    std::unique_lock<std::shared_mutex> lock(m_mtx);
    m_asyncFlushEnabled = enabled;
  }

  void StopBackgroundWorker() {
    {
      std::lock_guard<std::mutex> lock(m_workerMtx);
      if (!m_backgroundRunning) {
        return;
      }
      m_backgroundStop = true;
    }
    m_workerCv.notify_one();

    if (m_backgroundWorker.joinable()) {
      m_backgroundWorker.join();
    }

    std::lock_guard<std::mutex> lock(m_workerMtx);
    m_backgroundRunning = false;
  }

 private:
  enum class EntryType : uint8_t { Put = 1, Delete = 2 };

  struct Entry {
    EntryType type = EntryType::Put;
    std::string value;
  };

  struct SSTable {
    static constexpr std::size_t kBloomBitCount = 2048;
    static constexpr std::size_t kBloomWordCount = kBloomBitCount / 64;
    static constexpr std::size_t kBloomHashCount = 4;
    static constexpr std::size_t kBlockEntryCount = 16;

    struct BlockMeta {
      std::string firstKey;
      std::string lastKey;
      std::map<std::string, Entry>::const_iterator begin;
      std::map<std::string, Entry>::const_iterator end;
    };

    uint64_t sequence = 0;
    std::string minKey;
    std::string maxKey;
    std::string filePath;
    std::vector<uint64_t> bloomBits;
    std::vector<BlockMeta> blocks;
    std::map<std::string, Entry> entries;

    bool MayContain(const std::string& key) const {
      if (entries.empty()) {
        return false;
      }
      return key >= minKey && key <= maxKey && BloomMayContain(key);
    }

    void AddBloomKey(const std::string& key) {
      if (bloomBits.empty()) {
        bloomBits.assign(kBloomWordCount, 0);
      }
      auto h1 = std::hash<std::string>{}(key);
      auto h2 = std::hash<std::string>{}("lsm:" + key);
      for (std::size_t i = 0; i < kBloomHashCount; ++i) {
        std::size_t bit = (h1 + i * h2 + i * i) % kBloomBitCount;
        bloomBits[bit / 64] |= (uint64_t{1} << (bit % 64));
      }
    }

    bool BloomMayContain(const std::string& key) const {
      if (bloomBits.empty()) {
        return false;
      }
      auto h1 = std::hash<std::string>{}(key);
      auto h2 = std::hash<std::string>{}("lsm:" + key);
      for (std::size_t i = 0; i < kBloomHashCount; ++i) {
        std::size_t bit = (h1 + i * h2 + i * i) % kBloomBitCount;
        if ((bloomBits[bit / 64] & (uint64_t{1} << (bit % 64))) == 0) {
          return false;
        }
      }
      return true;
    }

    bool FindInIndexedBlock(const std::string& key, Entry* entry) const {
      for (const auto& block : blocks) {
        if (key < block.firstKey || key > block.lastKey) {
          continue;
        }
        for (auto it = block.begin; it != block.end; ++it) {
          if (it->first == key) {
            *entry = it->second;
            return true;
          }
          if (it->first > key) {
            return false;
          }
        }
        return false;
      }
      return false;
    }
  };

  struct WALRecord {
    EntryType type = EntryType::Put;
    std::string key;
    std::string value;
  };

  bool FindEntryLocked(const std::string& key, Entry* entry) const {
    auto memIt = m_memTable.find(key);
    if (memIt != m_memTable.end()) {
      *entry = memIt->second;
      return true;
    }

    for (auto it = m_immutableMemTables.rbegin(); it != m_immutableMemTables.rend(); ++it) {
      auto found = it->find(key);
      if (found != it->end()) {
        *entry = found->second;
        return true;
      }
    }

    for (auto it = m_sstables.rbegin(); it != m_sstables.rend(); ++it) {
      if (!it->MayContain(key)) {
        continue;
      }
      if (it->FindInIndexedBlock(key, entry)) {
        return true;
      }
    }

    return false;
  }

  std::map<std::string, std::string> ExportVisibleDataLocked() const {
    std::map<std::string, std::string> result;
    std::map<std::string, bool> seen;

    auto applyNewestFirst = [&result, &seen](const std::map<std::string, Entry>& entries) {
      for (const auto& item : entries) {
        if (seen[item.first]) {
          continue;
        }
        seen[item.first] = true;
        if (item.second.type == EntryType::Put) {
          result[item.first] = item.second.value;
        }
      }
    };

    applyNewestFirst(m_memTable);
    for (auto it = m_immutableMemTables.rbegin(); it != m_immutableMemTables.rend(); ++it) {
      applyNewestFirst(*it);
    }
    for (auto it = m_sstables.rbegin(); it != m_sstables.rend(); ++it) {
      applyNewestFirst(it->entries);
    }
    return result;
  }

  void FlushMemTableIfNeededLocked() {
    if (m_memTable.size() >= m_memTableFlushThreshold) {
      if (m_asyncFlushEnabled) {
        RotateMemTableToImmutableLocked();
        NotifyBackgroundWorker();
      } else {
        FlushMemTableLocked();
      }
    }
  }

  void FlushMemTableLocked() {
    RotateMemTableToImmutableLocked();
    FlushImmutableMemTablesLocked();
  }

  void RotateMemTableToImmutableLocked() {
    if (m_memTable.empty()) {
      return;
    }
    m_immutableMemTables.push_back(std::move(m_memTable));
    m_memTable.clear();
  }

  void FlushImmutableMemTablesLocked() {
    while (!m_immutableMemTables.empty()) {
      // 必须按从旧到新的顺序刷盘，避免旧的 immutable memtable 留在内存中覆盖更新的 SSTable。
      auto entries = std::move(m_immutableMemTables.front());
      m_immutableMemTables.erase(m_immutableMemTables.begin());

      SSTable table;
      table.sequence = ++m_nextSSTableSequence;
      table.entries = std::move(entries);
      RebuildSSTableMetadata(&table);
      PersistSSTableLocked(&table);
      m_sstables.push_back(std::move(table));
      RewriteManifestLocked();
    }
  }

  void CompactSSTablesLocked() {
    if (m_sstables.size() <= 1) {
      return;
    }

    std::map<std::string, Entry> merged;
    std::unordered_set<std::string> seen;
    for (auto tableIt = m_sstables.rbegin(); tableIt != m_sstables.rend(); ++tableIt) {
      for (const auto& item : tableIt->entries) {
        if (seen.find(item.first) != seen.end()) {
          continue;
        }
        seen.insert(item.first);
        if (item.second.type == EntryType::Put) {
          merged[item.first] = item.second;
        }
      }
    }

    if (merged.empty()) {
      DeleteSSTableFilesLocked();
      m_sstables.clear();
      RewriteManifestLocked();
      return;
    }

    SSTable compacted;
    compacted.sequence = ++m_nextSSTableSequence;
    compacted.entries = std::move(merged);
    RebuildSSTableMetadata(&compacted);
    PersistSSTableLocked(&compacted);
    DeleteSSTableFilesLocked();
    m_sstables.clear();
    m_sstables.push_back(std::move(compacted));
    RewriteManifestLocked();
  }

  static void RebuildSSTableMetadata(SSTable* table) {
    table->bloomBits.clear();
    table->blocks.clear();
    table->minKey.clear();
    table->maxKey.clear();
    if (table->entries.empty()) {
      return;
    }

    table->minKey = table->entries.begin()->first;
    table->maxKey = table->entries.rbegin()->first;
    for (const auto& item : table->entries) {
      table->AddBloomKey(item.first);
    }

    auto blockBegin = table->entries.cbegin();
    while (blockBegin != table->entries.cend()) {
      auto blockEnd = blockBegin;
      std::size_t count = 0;
      std::string lastKey = blockBegin->first;
      while (blockEnd != table->entries.cend() && count < SSTable::kBlockEntryCount) {
        lastKey = blockEnd->first;
        ++blockEnd;
        ++count;
      }

      table->blocks.push_back(SSTable::BlockMeta{blockBegin->first, lastKey, blockBegin, blockEnd});
      blockBegin = blockEnd;
    }
  }

  std::string SSTablePath(uint64_t sequence) const {
    if (m_manifestPath.empty()) {
      return "";
    }
    return m_manifestPath + ".sst." + std::to_string(sequence);
  }

  void PersistSSTableLocked(SSTable* table) {
    if (m_manifestPath.empty()) {
      return;
    }

    table->filePath = SSTablePath(table->sequence);
    std::ofstream out(table->filePath, std::ios::binary | std::ios::trunc);
    if (!out) {
      throw std::runtime_error("failed to write LSM SSTable");
    }

    WriteUint64ToStream(out, static_cast<uint64_t>(table->entries.size()));
    for (const auto& item : table->entries) {
      char type = static_cast<char>(item.second.type);
      out.write(&type, 1);
      WriteUint64ToStream(out, static_cast<uint64_t>(item.first.size()));
      WriteUint64ToStream(out, static_cast<uint64_t>(item.second.value.size()));
      out.write(item.first.data(), static_cast<std::streamsize>(item.first.size()));
      out.write(item.second.value.data(), static_cast<std::streamsize>(item.second.value.size()));
    }
    out.flush();
    if (!out) {
      throw std::runtime_error("failed to flush LSM SSTable");
    }
  }

  SSTable LoadSSTable(uint64_t sequence, const std::string& filePath) const {
    std::ifstream in(filePath, std::ios::binary);
    if (!in) {
      throw std::runtime_error("failed to open LSM SSTable");
    }

    SSTable table;
    table.sequence = sequence;
    table.filePath = filePath;

    uint64_t count = ReadUint64FromStream(in);
    for (uint64_t i = 0; i < count; ++i) {
      char rawType = 0;
      in.read(&rawType, 1);
      if (!in) {
        throw std::runtime_error("LSM SSTable has a truncated record type");
      }
      auto keySize = ReadUint64FromStream(in);
      auto valueSize = ReadUint64FromStream(in);

      Entry entry;
      entry.type = static_cast<EntryType>(static_cast<uint8_t>(rawType));
      if (entry.type != EntryType::Put && entry.type != EntryType::Delete) {
        throw std::runtime_error("LSM SSTable has an invalid record type");
      }

      std::string key(static_cast<std::size_t>(keySize), '\0');
      entry.value.resize(static_cast<std::size_t>(valueSize));
      in.read(key.data(), static_cast<std::streamsize>(key.size()));
      in.read(entry.value.data(), static_cast<std::streamsize>(entry.value.size()));
      if (!in) {
        throw std::runtime_error("LSM SSTable has a truncated record body");
      }
      table.entries[key] = std::move(entry);
    }

    RebuildSSTableMetadata(&table);
    return table;
  }

  void RewriteManifestLocked() {
    if (m_manifestPath.empty()) {
      return;
    }

    std::ofstream out(m_manifestPath, std::ios::binary | std::ios::trunc);
    if (!out) {
      throw std::runtime_error("failed to rewrite LSM manifest");
    }

    WriteUint64ToStream(out, m_nextSSTableSequence);
    WriteUint64ToStream(out, static_cast<uint64_t>(m_sstables.size()));
    for (const auto& table : m_sstables) {
      WriteUint64ToStream(out, table.sequence);
      WriteStreamString(out, table.filePath);
      WriteStreamString(out, table.minKey);
      WriteStreamString(out, table.maxKey);
    }
    out.flush();
    if (!out) {
      throw std::runtime_error("failed to flush LSM manifest");
    }
  }

  void RecoverFromManifestLocked() {
    if (m_manifestPath.empty()) {
      return;
    }

    std::ifstream in(m_manifestPath, std::ios::binary);
    if (!in.good()) {
      return;
    }

    m_nextSSTableSequence = ReadUint64FromStream(in);
    uint64_t tableCount = ReadUint64FromStream(in);
    for (uint64_t i = 0; i < tableCount; ++i) {
      uint64_t sequence = ReadUint64FromStream(in);
      std::string filePath = ReadStreamString(in);
      ReadStreamString(in);
      ReadStreamString(in);
      m_sstables.push_back(LoadSSTable(sequence, filePath));
    }
  }

  void DeleteSSTableFilesLocked() {
    if (m_manifestPath.empty()) {
      return;
    }
    for (const auto& table : m_sstables) {
      if (!table.filePath.empty()) {
        std::remove(table.filePath.c_str());
      }
    }
  }

  void NotifyBackgroundWorker() {
    {
      std::lock_guard<std::mutex> lock(m_workerMtx);
      m_backgroundWorkPending = true;
    }
    m_workerCv.notify_one();
  }

  void BackgroundWorkerLoop(int intervalMs) {
    while (true) {
      {
        std::unique_lock<std::mutex> lock(m_workerMtx);
        m_workerCv.wait_for(lock, std::chrono::milliseconds(intervalMs),
                            [this]() { return m_backgroundStop || m_backgroundWorkPending; });
        if (m_backgroundStop) {
          break;
        }
        m_backgroundWorkPending = false;
      }

      std::unique_lock<std::shared_mutex> lock(m_mtx);
      FlushMemTableLocked();
      if (m_sstables.size() > 1) {
        CompactSSTablesLocked();
      }
    }

    std::unique_lock<std::shared_mutex> lock(m_mtx);
    FlushMemTableLocked();
    if (m_sstables.size() > 1) {
      CompactSSTablesLocked();
    }
  }

  void RecoverFromWALLocked() {
    if (m_walPath.empty()) {
      return;
    }

    std::ifstream in(m_walPath, std::ios::binary);
    if (!in.good()) {
      return;
    }

    while (in.peek() != std::ifstream::traits_type::eof()) {
      WALRecord record = ReadWALRecord(in);
      m_memTable[record.key] = Entry{record.type, record.value};
      FlushMemTableIfNeededLocked();
    }
  }

  void AppendWALLocked(EntryType type, const std::string& key, const std::string& value) {
    if (m_walPath.empty()) {
      return;
    }

    std::ofstream out(m_walPath, std::ios::binary | std::ios::app);
    if (!out) {
      throw std::runtime_error("failed to open LSM WAL for append");
    }
    WriteWALRecord(out, WALRecord{type, key, value});
    out.flush();
    if (!out) {
      throw std::runtime_error("failed to write LSM WAL record");
    }
  }

  void RewriteWALLocked() {
    if (m_walPath.empty()) {
      return;
    }

    std::ofstream out(m_walPath, std::ios::binary | std::ios::trunc);
    if (!out) {
      throw std::runtime_error("failed to rewrite LSM WAL");
    }
    for (const auto& item : ExportVisibleDataLocked()) {
      WriteWALRecord(out, WALRecord{EntryType::Put, item.first, item.second});
    }
    out.flush();
    if (!out) {
      throw std::runtime_error("failed to flush rewritten LSM WAL");
    }
  }

  static void WriteWALRecord(std::ofstream& out, const WALRecord& record) {
    char type = static_cast<char>(record.type);
    out.write(&type, 1);
    WriteUint64ToStream(out, static_cast<uint64_t>(record.key.size()));
    WriteUint64ToStream(out, static_cast<uint64_t>(record.value.size()));
    out.write(record.key.data(), static_cast<std::streamsize>(record.key.size()));
    out.write(record.value.data(), static_cast<std::streamsize>(record.value.size()));
  }

  static WALRecord ReadWALRecord(std::ifstream& in) {
    char rawType = 0;
    in.read(&rawType, 1);
    if (!in) {
      throw std::runtime_error("LSM WAL has a truncated record type");
    }

    auto keySize = ReadUint64FromStream(in);
    auto valueSize = ReadUint64FromStream(in);

    WALRecord record;
    record.type = static_cast<EntryType>(static_cast<uint8_t>(rawType));
    if (record.type != EntryType::Put && record.type != EntryType::Delete) {
      throw std::runtime_error("LSM WAL has an invalid record type");
    }

    record.key.resize(static_cast<std::size_t>(keySize));
    record.value.resize(static_cast<std::size_t>(valueSize));
    in.read(record.key.data(), static_cast<std::streamsize>(record.key.size()));
    in.read(record.value.data(), static_cast<std::streamsize>(record.value.size()));
    if (!in) {
      throw std::runtime_error("LSM WAL has a truncated record body");
    }
    return record;
  }

  static void WriteUint64ToStream(std::ofstream& out, uint64_t value) {
    for (int i = 0; i < 8; ++i) {
      char byte = static_cast<char>((value >> (i * 8)) & 0xff);
      out.write(&byte, 1);
    }
  }

  static uint64_t ReadUint64FromStream(std::ifstream& in) {
    char bytes[8];
    in.read(bytes, 8);
    if (!in) {
      throw std::runtime_error("LSM WAL has a truncated integer");
    }
    uint64_t value = 0;
    for (int i = 0; i < 8; ++i) {
      value |= static_cast<uint64_t>(static_cast<unsigned char>(bytes[i])) << (i * 8);
    }
    return value;
  }

  static void WriteUint64(std::string* out, uint64_t value) {
    for (int i = 0; i < 8; ++i) {
      out->push_back(static_cast<char>((value >> (i * 8)) & 0xff));
    }
  }

  static uint64_t ReadUint64(const std::string& input, std::size_t* offset) {
    if (input.size() - *offset < 8) {
      throw std::runtime_error("LSM snapshot is truncated");
    }
    uint64_t value = 0;
    for (int i = 0; i < 8; ++i) {
      value |= static_cast<uint64_t>(static_cast<unsigned char>(input[*offset + i])) << (i * 8);
    }
    *offset += 8;
    return value;
  }

  static void WriteString(std::string* out, const std::string& value) {
    WriteUint64(out, static_cast<uint64_t>(value.size()));
    out->append(value);
  }

  static std::string ReadString(const std::string& input, std::size_t* offset) {
    uint64_t size = ReadUint64(input, offset);
    if (size > input.size() - *offset) {
      throw std::runtime_error("LSM snapshot string is truncated");
    }
    std::string value = input.substr(*offset, static_cast<std::size_t>(size));
    *offset += static_cast<std::size_t>(size);
    return value;
  }

  static void WriteStreamString(std::ofstream& out, const std::string& value) {
    WriteUint64ToStream(out, static_cast<uint64_t>(value.size()));
    out.write(value.data(), static_cast<std::streamsize>(value.size()));
  }

  static std::string ReadStreamString(std::ifstream& in) {
    uint64_t size = ReadUint64FromStream(in);
    std::string value(static_cast<std::size_t>(size), '\0');
    in.read(value.data(), static_cast<std::streamsize>(value.size()));
    if (!in) {
      throw std::runtime_error("LSM file string is truncated");
    }
    return value;
  }

  std::size_t m_memTableFlushThreshold;
  uint64_t m_nextSSTableSequence = 0;
  std::string m_walPath;
  std::string m_manifestPath;
  mutable std::shared_mutex m_mtx;
  bool m_asyncFlushEnabled = false;
  std::map<std::string, Entry> m_memTable;
  std::vector<std::map<std::string, Entry>> m_immutableMemTables;
  std::vector<SSTable> m_sstables;
  std::mutex m_workerMtx;
  std::condition_variable m_workerCv;
  std::thread m_backgroundWorker;
  bool m_backgroundStop = false;
  bool m_backgroundRunning = false;
  bool m_backgroundWorkPending = false;
};

#endif  // KV_RAFT_STORAGE_LSM_TREE_ENGINE_H
