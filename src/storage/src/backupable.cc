//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include "storage/backupable.h"

#include <dirent.h>
#include <cassert>
#include <utility>

#include "glog/logging.h"
#include "rocksdb/db.h"
#include "rocksdb/utilities/checkpoint.h"
#include "storage/storage.h"

namespace storage {

BackupEngine::~BackupEngine() {
  // Wait all children threads
  WaitBackupPthread();
}

Status BackupEngine::Open(std::shared_ptr<Storage> storage, BackupEngine** backup_engine_ptr) {
  *backup_engine_ptr = new BackupEngine(storage);
  return Status::OK();
}

Status BackupEngine::CreateNewBackupSpecify(const std::string& backup_dir, const std::string& type) {
  rocksdb::DB* db = storage_->GetDBByType(type);

  if (db == nullptr) {
    return Status::Corruption("db is nullptr for type:%s", type.c_str());
  }

  std::string dir = GetSaveDirByType(backup_dir, type);
  delete_dir(dir.c_str());

  Status s;
  rocksdb::Checkpoint* checkpoint;
  s = rocksdb::Checkpoint::Create(db, &checkpoint);
  assert(s.ok());
  s = checkpoint->CreateCheckpoint(dir);

  if (!s.ok()) {
    LOG(WARNING) << "error for create checkpoint, db type: " << type << "; dir:" << dir << "; status " << s.ToString();
  }

  return s;
}

void* ThreadFuncSaveSpecify(void* arg) {
  BackupSaveArgs* arg_ptr = static_cast<BackupSaveArgs*>(arg);
  BackupEngine* p = static_cast<BackupEngine*>(arg_ptr->p_engine);
  arg_ptr->res = p->CreateNewBackupSpecify(arg_ptr->backup_dir, arg_ptr->key_type);
  pthread_exit(&(arg_ptr->res));
}

Status BackupEngine::WaitBackupPthread() {
  int ret;
  Status s = Status::OK();
  for (auto& pthread : backup_pthread_ts_) {
    void* res;
    if ((ret = pthread_join(pthread.second, &res)) != 0) {
    }
    Status cur_s = *(static_cast<Status*>(res));
    if (!cur_s.ok()) {
      s = cur_s;
    }
  }
  backup_pthread_ts_.clear();
  return s;
}

Status BackupEngine::CreateNewBackup(const std::string& dir) {
  Status s;
  std::string types[] = {STRINGS_DB, HASHES_DB, LISTS_DB, ZSETS_DB, SETS_DB};

  std::vector<BackupSaveArgs*> args;
  for (const auto& type : types) {
    pthread_t tid;
    BackupSaveArgs* arg = new BackupSaveArgs(reinterpret_cast<void*>(this), dir, type);
    args.push_back(arg);
    // TODO:: add thread pool for pika, use for bg work
    if (pthread_create(&tid, nullptr, &ThreadFuncSaveSpecify, arg) != 0) {
      s = Status::Corruption("pthead_create failed.");
      LOG(WARNING) << "pthread create fail for backup db type:" << type;
      break;
    }

    if (!(backup_pthread_ts_.insert(std::make_pair(type, tid)).second)) {
      backup_pthread_ts_[type] = tid;
    }
  }

  // TODO: collect all thread status
  Status wait = WaitBackupPthread();
  if (!wait.ok()) {
    s = wait;
  }

  for (auto& a : args) {
    delete a;
  }

  return s;
}

}  // namespace storage

