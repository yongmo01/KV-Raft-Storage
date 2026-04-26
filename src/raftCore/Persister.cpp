//
// Created by swx on 23-5-30.
//
#include "Persister.h"
#include "util.h"

#include <sstream>

namespace {

std::string ReadWholeFile(const std::string& fileName) {
  std::ifstream ifs(fileName, std::ios::in | std::ios::binary);
  if (!ifs.good()) {
    return "";
  }

  std::ostringstream oss;
  oss << ifs.rdbuf();
  return oss.str();
}

long long GetFileSize(const std::string& fileName) {
  std::ifstream ifs(fileName, std::ios::in | std::ios::binary | std::ios::ate);
  if (!ifs.good()) {
    return 0;
  }
  return static_cast<long long>(ifs.tellg());
}

bool EnsureFileExists(const std::string& fileName) {
  // 只创建不存在的文件，不能在构造 Persister 时清空已有持久化状态。
  std::ofstream ofs(fileName, std::ios::out | std::ios::app | std::ios::binary);
  return ofs.is_open();
}

}  // namespace

void Persister::Save(const std::string raftstate, const std::string snapshot) {
  std::lock_guard<std::mutex> lg(m_mtx);
  clearRaftStateAndSnapshot();
  // 将 Raft 状态和状态机快照一起落盘，保证快照和日志截断点匹配。
  m_raftStateOutStream << raftstate;
  m_snapshotOutStream << snapshot;
  m_raftStateOutStream.flush();
  m_snapshotOutStream.flush();
  m_raftStateSize = static_cast<long long>(raftstate.size());
}

std::string Persister::ReadSnapshot() {
  std::lock_guard<std::mutex> lg(m_mtx);
  if (m_snapshotOutStream.is_open()) {
    m_snapshotOutStream.close();
  }

  DEFER {
    m_snapshotOutStream.open(m_snapshotFileName, std::ios::out | std::ios::app | std::ios::binary);
  };
  return ReadWholeFile(m_snapshotFileName);
}

void Persister::SaveRaftState(const std::string &data) {
  std::lock_guard<std::mutex> lg(m_mtx);
  // 只更新 Raft 状态，不改变已有快照。
  clearRaftState();
  m_raftStateOutStream << data;
  m_raftStateOutStream.flush();
  m_raftStateSize = static_cast<long long>(data.size());
}

long long Persister::RaftStateSize() {
  std::lock_guard<std::mutex> lg(m_mtx);

  return m_raftStateSize;
}

std::string Persister::ReadRaftState() {
  std::lock_guard<std::mutex> lg(m_mtx);//函数构造时加锁，函数结束时自动解锁。当函数崩溃的时候，防止死锁，保证其他线程能够继续访问这个函数

  return ReadWholeFile(m_raftStateFileName);
}

Persister::Persister(const int me)
    : m_raftStateFileName("raftstatePersist" + std::to_string(me) + ".txt"),
      m_snapshotFileName("snapshotPersist" + std::to_string(me) + ".txt"),
      m_raftStateSize(0) {
  /**
   * 检查并创建持久化文件。
   * 这里不能使用 std::ios::trunc，否则节点重启时会把已有 Raft 状态和快照清空。
   */
  bool fileOpenFlag = true;
  if (!EnsureFileExists(m_raftStateFileName)) {
    fileOpenFlag = false;
  }
  if (!EnsureFileExists(m_snapshotFileName)) {
    fileOpenFlag = false;
  }
  if (!fileOpenFlag) {
    DPrintf("[func-Persister::Persister] file open error");
  }
  m_raftStateSize = GetFileSize(m_raftStateFileName);
  /**
   * 绑定输出流。构造阶段使用 app 模式，避免打开流时截断已有文件。
   * 真正保存新状态时会在 clearRaftState/clearSnapshot 中显式截断。
   */
  m_raftStateOutStream.open(m_raftStateFileName, std::ios::out | std::ios::app | std::ios::binary);
  m_snapshotOutStream.open(m_snapshotFileName, std::ios::out | std::ios::app | std::ios::binary);
}

Persister::~Persister() {
  if (m_raftStateOutStream.is_open()) {
    m_raftStateOutStream.close();
  }
  if (m_snapshotOutStream.is_open()) {
    m_snapshotOutStream.close();
  }
}

void Persister::clearRaftState() {
  m_raftStateSize = 0;
  // 关闭文件流
  if (m_raftStateOutStream.is_open()) {
    m_raftStateOutStream.close();
  }
  // 重新打开文件流并清空文件内容
  m_raftStateOutStream.open(m_raftStateFileName, std::ios::out | std::ios::trunc | std::ios::binary);
}

void Persister::clearSnapshot() {
  if (m_snapshotOutStream.is_open()) {
    m_snapshotOutStream.close();
  }
  m_snapshotOutStream.open(m_snapshotFileName, std::ios::out | std::ios::trunc | std::ios::binary);
}

void Persister::clearRaftStateAndSnapshot() {
  clearRaftState();
  clearSnapshot();
}
