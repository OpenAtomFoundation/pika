//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_BACKUPABLE_H_
#define SRC_BACKUPABLE_H_

#include "rocksdb/db.h"

#include "db_checkpoint.h"
#include "storage.h"
#include "util.h"

namespace storage {

const std::string DEFAULT_BK_PATH = "dump";  // Default backup root dir
const std::string DEFAULT_RS_PATH = "db";    // Default restore root dir

// Arguments which will used by BackupSave Thread
// p_engine for BackupEngine handler
// backup_dir
// key_type kv, hash, list, set or zset
struct BackupSaveArgs {
  void* p_engine;
  const std::string backup_dir;
  const std::string key_type;
  Status res;

  BackupSaveArgs(void* _p_engine, const std::string& _backup_dir, const std::string& _key_type)
      : p_engine(_p_engine), backup_dir(_backup_dir), key_type(_key_type) {}
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
  static Status Open(Storage* db, BackupEngine** backup_engine_ptr);

  Status SetBackupContent();

  Status CreateNewBackup(const std::string& dir);

  void StopBackup();

  Status CreateNewBackupSpecify(const std::string& dir, const std::string& type);

 private:
  BackupEngine() {}

  std::map<std::string, rocksdb::DBCheckpoint*> engines_;
  std::map<std::string, BackupContent> backup_content_;
  std::map<std::string, pthread_t> backup_pthread_ts_;

  Status NewCheckpoint(rocksdb::DB* rocksdb_db, const std::string& type);
  std::string GetSaveDirByType(const std::string _dir, const std::string& _type) const {
    std::string backup_dir = _dir.empty() ? DEFAULT_BK_PATH : _dir;
    return backup_dir + ((backup_dir.back() != '/') ? "/" : "") + _type;
  }
  Status WaitBackupPthread();
};

}  //  namespace storage
#endif  //  SRC_BACKUPABLE_H_
