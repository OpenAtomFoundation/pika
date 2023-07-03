//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include "storage/backupable.h"

#include <dirent.h>

#include <utility>

namespace storage {

BackupEngine::~BackupEngine() {
  // Wait all children threads
  StopBackup();
  WaitBackupPthread();
}

Status BackupEngine::NewCheckpoint(rocksdb::DB* rocksdb_db,
                                   const std::string& type) {
  rocksdb::DBCheckpoint* checkpoint;
  Status s = rocksdb::DBCheckpoint::Create(rocksdb_db, &checkpoint);
  if (!s.ok()) {
    return s;
  }
  engines_.insert(
      std::make_pair(type, std::unique_ptr<rocksdb::DBCheckpoint>(checkpoint)));
  return s;
}

Status BackupEngine::Open(storage::Storage* storage,
                          std::shared_ptr<BackupEngine>& backup_engine_ret) {
  // BackupEngine() is private, can't use make_shared
  backup_engine_ret = std::shared_ptr<BackupEngine>(new BackupEngine());
  if (!backup_engine_ret) {
    return Status::Corruption("New BackupEngine failed!");
  }

  // Create BackupEngine for each db type
  rocksdb::Status s;
  rocksdb::DB* rocksdb_db;
  std::string types[] = {STRINGS_DB, HASHES_DB, LISTS_DB, ZSETS_DB, SETS_DB};
  for (const auto& type : types) {
    if (!(rocksdb_db = storage->GetDBByType(type))) {
      s = Status::Corruption("Error db type");
    }

    if (s.ok()) {
      s = backup_engine_ret->NewCheckpoint(rocksdb_db, type);
    }

    if (!s.ok()) {
      backup_engine_ret = nullptr;
      break;
    }
  }
  return s;
}

Status BackupEngine::SetBackupContent() {
  Status s;
  for (const auto& engine : engines_) {
    // Get backup content
    BackupContent bcontent;
    s = engine.second->GetCheckpointFiles(
        bcontent.live_files, bcontent.live_wal_files,
        bcontent.manifest_file_size, bcontent.sequence_number);
    if (!s.ok()) {
      return s;
    }
    backup_content_[engine.first] = std::move(bcontent);
  }
  return s;
}

Status BackupEngine::CreateNewBackupSpecify(const std::string& backup_dir,
                                            const std::string& type) {
  auto it_engine = engines_.find(type);
  auto it_content = backup_content_.find(type);
  std::string dir = GetSaveDirByType(backup_dir, type);
  delete_dir(dir.c_str());

  if (it_content != backup_content_.end() && it_engine != engines_.end()) {
    Status s = it_engine->second->CreateCheckpointWithFiles(
        dir, it_content->second.live_files, it_content->second.live_wal_files,
        it_content->second.manifest_file_size,
        it_content->second.sequence_number);
    if (!s.ok()) {
      //    type.c_str(), s.ToString().c_str());
      return s;
    }

  } else {
    return Status::Corruption("invalid db type");
  }
  return Status::OK();
}

void* ThreadFuncSaveSpecify(void* arg) {
  auto arg_ptr = static_cast<BackupSaveArgs*>(arg);
  auto p = static_cast<BackupEngine*>(arg_ptr->p_engine);
  arg_ptr->res =
      p->CreateNewBackupSpecify(arg_ptr->backup_dir, arg_ptr->key_type);
  pthread_exit(&(arg_ptr->res));
}

Status BackupEngine::WaitBackupPthread() {
  int ret;
  Status s = Status::OK();
  for (auto& pthread : backup_pthread_ts_) {
    void* res;
    if (pthread_join(pthread.second, &res) != 0) {
    }
    Status cur_s = *(static_cast<Status*>(res));
    if (!cur_s.ok()) {
      StopBackup();  // stop others when someone failed
      s = cur_s;
    }
  }
  backup_pthread_ts_.clear();
  return s;
}

Status BackupEngine::CreateNewBackup(const std::string& dir) {
  Status s = Status::OK();
  // ensure cleaning up the pointers after the function has finished.
  std::vector<std::unique_ptr<BackupSaveArgs>> args;
  args.reserve(engines_.size());
  for (const auto& engine : engines_) {
    pthread_t tid;
    auto arg = std::make_unique<BackupSaveArgs>(reinterpret_cast<void*>(this),
                                                dir, engine.first);
    args.push_back(std::move(arg));
    if (pthread_create(&tid, nullptr, &ThreadFuncSaveSpecify,
                       args.back().get()) != 0) {
      s = Status::Corruption("pthread_create failed.");
      break;
    }
    if (!(backup_pthread_ts_.insert(std::make_pair(engine.first, tid))
              .second)) {
      backup_pthread_ts_[engine.first] = tid;
    }
  }

  // Wait threads stop
  if (!s.ok()) {
    StopBackup();
  }
  s = WaitBackupPthread();

  return s;
}

void BackupEngine::StopBackup() {
  // DEPRECATED
}

}  // namespace storage
