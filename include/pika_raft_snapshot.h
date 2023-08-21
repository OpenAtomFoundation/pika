// Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_RAFT_SNAPSHOT_H_
#define PIKA_RAFT_SNAPSHOT_H_

#include <fstream>
#include <set>

#include <libnuraft/nuraft.hxx>

#include "include/pika_define.h"
#include "include/pika_conf.h"

using namespace pstd;

class SnapshotFileWriter {
 public:
  SnapshotFileWriter(const std::string& filepath);
  ~SnapshotFileWriter() {}
  Status Write(uint64_t offset, size_t n, const char* data);
  Status Close();
  Status Fsync();
 private:
  std::string   filepath_;
  int           fd_ = -1;
};

class RaftSnapshotCreater {
 public:
  RaftSnapshotCreater();
  ~RaftSnapshotCreater() {}
  void Clear();
  bool CreateSnapshotMeta(nuraft::ptr<nuraft::buffer>& snapshot_content);
  bool CreateNextSnapshotContent(nuraft::ptr<nuraft::buffer>& snapshot_content);
 private:
  nuraft::ptr<nuraft::buffer> InternalCreateSnapshot(const std::string db_name, const uint32_t slot_id, const std::string filename);
  void SerializeSnapshot(char* buffer, size_t bytes_read, size_t offset, bool is_eof
                , std::string checksum, std::string filename
                , std::string db_name, uint32_t slot_id
                , nuraft::ptr<nuraft::buffer>& snapshot_content
                );
  void SerializeSnapshotMeta(std::string db_name, uint32_t slot_id
                , std::set<std::string>& filenames
                , nuraft::ptr<nuraft::buffer>& snapshot_content
                );
  Status ReadDumpFile(const std::string filepath, const size_t offset, const size_t count,
                    char* data, size_t* bytes_read, std::string* checksum);

  std::vector<DBStruct>               db_structs_;
  std::vector<DBStruct>::iterator     db_it_;
  std::set<uint32_t>::iterator        slot_id_it_;
  std::vector<std::string>               filenames_;
  std::vector<std::string>::iterator  filename_it_;
  size_t                              offset_;
  bool                                is_eof_;
  bool                                done_;
};

class RaftSnapshotManager {
 public:
  RaftSnapshotManager();
  ~RaftSnapshotManager() {}
  void DoBgsave(nuraft::ptr<nuraft::snapshot> ss);
  void StartCreateSnapshot();
  bool CreateSnapshotMeta(nuraft::ptr<nuraft::buffer>& snapshot_content);
  bool CreateNextSnapshotContent(nuraft::ptr<nuraft::buffer>& snapshot_content);
  void InstallSnapshotMeta(nuraft::snapshot& s, nuraft::buffer& snapshot_content);
  void PrepareInstallSnapshot();
  Status InstallSnapshot(nuraft::buffer& snapshot_content);
  Status ApplySnapshot();
  void SetLastSnapshot(nuraft::ptr<nuraft::snapshot> snapshot);
  nuraft::ptr<nuraft::snapshot> GetLastSnapshot();

 private:
  void DeserializeSnapshot(nuraft::buffer& snapshot_content
                , char* buffer, size_t& bytes_read, size_t& offset, bool& is_eof
                , std::string& checksum, std::string& filename
                , std::string& db_name, uint32_t& slot_id 
                );
  void PrepareInstallSnapshotInternal(std::string db_name, uint32_t slot_id);
  void BgsaveSnapshotInfo(const std::string& db_dump_path, nuraft::ptr<nuraft::snapshot> ss);
  Status LoadLocalSnapshotInfo(const std::string& db_path, nuraft::ptr<nuraft::snapshot> ss);
  std::string DbSyncPath(const std::string& sync_path, const std::string& db_name, const uint32_t slot_id);

  std::unique_ptr<RaftSnapshotCreater>        snapshot_creater_;
  std::map<std::string, std::set<uint32_t>>   snapshot_meta_;
  nuraft::ptr<nuraft::snapshot>               last_snapshot_;
};

#endif