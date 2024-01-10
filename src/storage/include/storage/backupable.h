//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_BACKUPABLE_H_
#define SRC_BACKUPABLE_H_

#include <utility>

#include "rocksdb/db.h"

#include "db_checkpoint.h"
#include "storage.h"
#include "util.h"

namespace storage {

inline const std::string DEFAULT_BK_PATH = "dump";  // Default backup root dir
inline const std::string DEFAULT_RS_PATH = "db";    // Default restore root dir

// Arguments which will used by BackupSave Thread
// p_engine for BackupEngine handler
// backup_dir
struct BackupSaveArgs {
  void* p_engine = nullptr;
  const std::string backup_dir;
  int index_ = 0;
  Status res;

  BackupSaveArgs(void* _p_engine, std::string  _backup_dir, int index)
      : p_engine(_p_engine), backup_dir(std::move(_backup_dir)), index_(index) {}
};

struct BackupContent {
  std::vector<std::string> live_files;
  rocksdb::VectorLogPtr live_wal_files;
  uint64_t manifest_file_size = 0;
  uint64_t sequence_number = 0;
};

class BackupEngine {
 public:
  ~BackupEngine();
  static Status Open(Storage* db, std::shared_ptr<BackupEngine>& backup_engine_ret, int inst_count);

  Status SetBackupContent();

  Status CreateNewBackup(const std::string& dir);

  void StopBackup();

  Status CreateNewBackupSpecify(const std::string& dir, int index);

 private:
  BackupEngine() = default;

  std::map<int, std::unique_ptr<rocksdb::DBCheckpoint>> engines_;
  std::map<int, BackupContent> backup_content_;
  std::map<int, pthread_t> backup_pthread_ts_;

  Status NewCheckpoint(rocksdb::DB* rocksdb_db, int index);
  std::string GetSaveDirByIndex(const std::string& _dir, int index) const {
    std::string backup_dir = _dir.empty() ? DEFAULT_BK_PATH : _dir;
    return backup_dir + ((backup_dir.back() != '/') ? "/" : "") + std::to_string(index);
  }
  Status WaitBackupPthread();
};

}  //  namespace storage
#endif  //  SRC_BACKUPABLE_H_

