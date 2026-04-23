#include "lsm_tree_engine.h"

#include <cassert>
#include <chrono>
#include <cstdio>
#include <string>
#include <thread>
#include <vector>

void RemoveLSMTestFiles(const std::string& manifestPath) {
  std::remove(manifestPath.c_str());
  for (int i = 1; i <= 16; ++i) {
    std::remove((manifestPath + ".sst." + std::to_string(i)).c_str());
  }
}

void TestPutGetAndFlush() {
  LSMTreeEngine engine(2);
  std::string value;

  engine.Put("a", "1");
  assert(engine.Get("a", &value));
  assert(value == "1");

  engine.Put("b", "2");
  assert(engine.Get("a", &value));
  assert(value == "1");
  assert(engine.Get("b", &value));
  assert(value == "2");
  assert(engine.Size() == 2);
}

void TestNewestValueWinsAcrossSSTables() {
  LSMTreeEngine engine(1);
  std::string value;

  engine.Put("k", "v1");
  engine.Put("k", "v2");
  assert(engine.Get("k", &value));
  assert(value == "v2");
}

void TestTombstoneHidesOldValue() {
  LSMTreeEngine engine(1);
  std::string value;

  engine.Put("k", "v1");
  engine.Delete("k");
  assert(!engine.Get("k", &value));
  assert(engine.Size() == 0);
}

void TestSnapshotRestore() {
  LSMTreeEngine engine(2);
  std::string value;

  engine.Put("a", "1");
  engine.Put("b", "2");
  engine.Delete("a");
  engine.Put("c", "");

  auto snapshot = engine.Snapshot();
  LSMTreeEngine restored(1);
  restored.Restore(snapshot);

  assert(!restored.Get("a", &value));
  assert(restored.Get("b", &value));
  assert(value == "2");
  assert(restored.Get("c", &value));
  assert(value.empty());
  assert(restored.Size() == 2);
}

void TestCompactionMergesSSTables() {
  LSMTreeEngine engine(1);
  std::string value;

  engine.Put("a", "old");
  engine.Put("b", "keep");
  engine.Put("a", "new");
  assert(engine.SSTableCount() == 3);

  engine.Compact();
  assert(engine.SSTableCount() == 1);
  assert(engine.Get("a", &value));
  assert(value == "new");
  assert(engine.Get("b", &value));
  assert(value == "keep");
}

void TestCompactionDropsDeletedKeys() {
  LSMTreeEngine engine(1);
  std::string value;

  engine.Put("a", "old");
  engine.Delete("a");
  engine.Put("b", "keep");
  assert(engine.SSTableCount() == 3);

  engine.Compact();
  assert(engine.SSTableCount() == 1);
  assert(!engine.Get("a", &value));
  assert(engine.Get("b", &value));
  assert(value == "keep");
  assert(engine.Size() == 1);
}

void TestWALRecovery() {
  const std::string walPath = "test_lsm_wal.bin";
  std::remove(walPath.c_str());

  {
    LSMTreeEngine engine(2, walPath);
    engine.Put("a", "1");
    engine.Put("b", "2");
    engine.Delete("a");
    engine.Put("c", "");
  }

  LSMTreeEngine recovered(2, walPath);
  std::string value;
  assert(!recovered.Get("a", &value));
  assert(recovered.Get("b", &value));
  assert(value == "2");
  assert(recovered.Get("c", &value));
  assert(value.empty());
  assert(recovered.Size() == 2);

  std::remove(walPath.c_str());
}

void TestManifestAndSSTableRecovery() {
  const std::string walPath = "test_lsm_manifest_wal.bin";
  const std::string manifestPath = "test_lsm_manifest.bin";
  std::remove(walPath.c_str());
  RemoveLSMTestFiles(manifestPath);

  {
    LSMTreeEngine engine(1, walPath, manifestPath);
    engine.Put("a", "old");
    engine.Put("b", "keep");
    engine.Put("a", "new");
    engine.Delete("c");
    engine.Compact();
  }

  LSMTreeEngine recovered(4, "", manifestPath);
  std::string value;
  assert(recovered.Get("a", &value));
  assert(value == "new");
  assert(recovered.Get("b", &value));
  assert(value == "keep");
  assert(!recovered.Get("c", &value));
  assert(recovered.SSTableCount() == 1);
  assert(recovered.Size() == 2);

  std::remove(walPath.c_str());
  RemoveLSMTestFiles(manifestPath);
}

void TestBlockIndexLookup() {
  LSMTreeEngine engine(32);
  std::string value;

  for (int i = 0; i < 40; ++i) {
    std::string suffix = i < 10 ? "0" + std::to_string(i) : std::to_string(i);
    engine.Put("key" + suffix, "value" + suffix);
  }
  engine.Flush();

  assert(engine.Get("key00", &value));
  assert(value == "value00");
  assert(engine.Get("key17", &value));
  assert(value == "value17");
  assert(engine.Get("key39", &value));
  assert(value == "value39");
  assert(!engine.Get("key40", &value));
}

void TestRangeScan() {
  LSMTreeEngine engine(1);

  engine.Put("a", "1");
  engine.Put("b", "old");
  engine.Put("c", "3");
  engine.Delete("a");
  engine.Put("b", "2");
  engine.Put("d", "4");

  auto range = engine.RangeScan("a", "c");
  assert(range.size() == 2);
  assert(range[0].first == "b");
  assert(range[0].second == "2");
  assert(range[1].first == "c");
  assert(range[1].second == "3");

  auto tail = engine.RangeScan("c", "z");
  assert(tail.size() == 2);
  assert(tail[0].first == "c");
  assert(tail[1].first == "d");

  auto empty = engine.RangeScan("z", "a");
  assert(empty.empty());
}

void TestBackgroundWorkerLifecycle() {
  LSMTreeEngine engine(100);
  std::string value;

  engine.StartBackgroundWorker(10);
  engine.Put("a", "1");
  engine.Put("b", "2");
  std::this_thread::sleep_for(std::chrono::milliseconds(30));
  engine.StopBackgroundWorker();

  assert(engine.SSTableCount() == 1);
  assert(engine.Get("a", &value));
  assert(value == "1");
  assert(engine.Get("b", &value));
  assert(value == "2");

  engine.StartBackgroundWorker(10);
  engine.StopBackgroundWorker();
}

int main() {
  TestPutGetAndFlush();
  TestNewestValueWinsAcrossSSTables();
  TestTombstoneHidesOldValue();
  TestSnapshotRestore();
  TestCompactionMergesSSTables();
  TestCompactionDropsDeletedKeys();
  TestWALRecovery();
  TestManifestAndSSTableRecovery();
  TestBlockIndexLookup();
  TestRangeScan();
  TestBackgroundWorkerLifecycle();
  return 0;
}
