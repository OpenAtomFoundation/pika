// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_stable_log.h"

#include <glog/logging.h>

#include <memory>
#include <utility>

#include "include/pika_conf.h"
#include "include/pika_rm.h"
#include "include/pika_server.h"

#include "pstd/include/env.h"

using pstd::Status;

extern PikaServer* g_pika_server;
extern std::unique_ptr<PikaReplicaManager> g_pika_rm;

StableLog::StableLog(std::string db_name, uint32_t slot_id, std::string log_path)
    : purging_(false), db_name_(std::move(db_name)), slot_id_(slot_id), log_path_(std::move(log_path)) {
  stable_logger_ = std::make_shared<Binlog>(log_path_, g_pika_conf->binlog_file_size());
  std::map<uint32_t, std::string> binlogs;
  if (!GetBinlogFiles(&binlogs)) {
    LOG(FATAL) << log_path_ << " Could not get binlog files!";
  }
  if (!binlogs.empty()) {
    UpdateFirstOffset(binlogs.begin()->first);
  }
}

StableLog::~StableLog() = default;

void StableLog::Leave() {
  Close();
  RemoveStableLogDir();
}

void StableLog::Close() { stable_logger_->Close(); }

void StableLog::RemoveStableLogDir() {
  std::string logpath = log_path_;
  if (logpath[logpath.length() - 1] == '/') {
    logpath.erase(logpath.length() - 1);
  }
  logpath.append("_deleting/");
  if (pstd::RenameFile(log_path_, logpath) != 0) {
    LOG(WARNING) << "Failed to move log to trash, error: " << strerror(errno);
    return;
  }
  g_pika_server->PurgeDir(logpath);

  LOG(WARNING) << "Slot StableLog: " << db_name_ << ":" << slot_id_ << " move to trash success";
}

bool StableLog::PurgeStableLogs(uint32_t to, bool manual) {
  // Only one thread can go through
  bool expect = false;
  if (!purging_.compare_exchange_strong(expect, true)) {
    LOG(WARNING) << "purge process already exist";
    return false;
  }
  auto arg = new PurgeStableLogArg();
  arg->to = to;
  arg->manual = manual;
  arg->logger = shared_from_this();
  g_pika_server->PurgelogsTaskSchedule(&DoPurgeStableLogs, static_cast<void*>(arg));
  return true;
}

void StableLog::ClearPurge() { purging_ = false; }

void StableLog::DoPurgeStableLogs(void* arg) {
  std::unique_ptr<PurgeStableLogArg> purge_arg(static_cast<PurgeStableLogArg*>(arg));
  purge_arg->logger->PurgeFiles(purge_arg->to, purge_arg->manual);
  purge_arg->logger->ClearPurge();
}

bool StableLog::PurgeFiles(uint32_t to, bool manual) {
  std::map<uint32_t, std::string> binlogs;
  if (!GetBinlogFiles(&binlogs)) {
    LOG(WARNING) << log_path_ << " Could not get binlog files!";
    return false;
  }

  int delete_num = 0;
  struct stat file_stat;
  int remain_expire_num = binlogs.size() - g_pika_conf->expire_logs_nums();
  std::shared_ptr<SyncMasterSlot> master_slot = nullptr;
  std::map<uint32_t, std::string>::iterator it;
  for (it = binlogs.begin(); it != binlogs.end(); ++it) {
    if ((manual && it->first <= to)           // Manual purgelogsto
        || (remain_expire_num > 0)            // Expire num trigger
        || (binlogs.size() - delete_num > 10  // At lease remain 10 files
            && stat(((log_path_ + it->second)).c_str(), &file_stat) == 0 &&
            file_stat.st_mtime < time(nullptr) - g_pika_conf->expire_logs_days() * 24 * 3600)) {  // Expire time trigger
      // We check this every time to avoid lock when we do file deletion
      master_slot = g_pika_rm->GetSyncMasterSlotByName(SlotInfo(db_name_, slot_id_));
      if (!master_slot) {
        LOG(WARNING) << "Slot: " << db_name_ << ":" << slot_id_ << " Not Found";
        return false;
      }

      if (!master_slot->BinlogCloudPurge(it->first)) {
        LOG(WARNING) << log_path_ << " Could not purge " << (it->first) << ", since it is already be used";
        return false;
      }

      // Do delete
      if (pstd::DeleteFile(log_path_ + it->second)) {
        ++delete_num;
        --remain_expire_num;
      } else {
        LOG(WARNING) << log_path_ << " Purge log file : " << (it->second) << " failed! error: delete file failed";
      }
    } else {
      // Break when face the first one not satisfied
      // Since the binlogs is order by the file index
      break;
    }
  }
  if (delete_num != 0) {
    std::map<uint32_t, std::string> binlogs;
    if (!GetBinlogFiles(&binlogs)) {
      LOG(WARNING) << log_path_ << " Could not get binlog files!";
      return false;
    }
    auto it = binlogs.begin();
    if (it != binlogs.end()) {
      UpdateFirstOffset(it->first);
    }
  }
  if (delete_num != 0) {
    LOG(INFO) << log_path_ << " Success purge " << delete_num << " binlog file";
  }
  return true;
}

bool StableLog::GetBinlogFiles(std::map<uint32_t, std::string>* binlogs) {
  std::vector<std::string> children;
  int ret = pstd::GetChildren(log_path_, children);
   if (ret) {
    LOG(WARNING) << log_path_ << " Get all files in log path failed! error:" << ret;
    return false;
  }

  int64_t index = 0;
  std::string sindex;
  std::vector<std::string>::iterator it;
  for (it = children.begin(); it != children.end(); ++it) {
    if ((*it).compare(0, kBinlogPrefixLen, kBinlogPrefix) != 0) {
      continue;
    }
    sindex = (*it).substr(kBinlogPrefixLen);
    if (pstd::string2int(sindex.c_str(), sindex.size(), &index) == 1) {
      binlogs->insert(std::pair<uint32_t, std::string>(static_cast<uint32_t>(index), *it));
    }
  }
  return true;
}

void StableLog::UpdateFirstOffset(uint32_t filenum) {
  PikaBinlogReader binlog_reader;
  int res = binlog_reader.Seek(stable_logger_, filenum, 0);
  if (res != 0) {
    LOG(WARNING) << "Binlog reader init failed";
    return;
  }

  BinlogItem item;
  BinlogOffset offset;
  while (true) {
    std::string binlog;
    Status s = binlog_reader.Get(&binlog, &(offset.filenum), &(offset.offset));
    if (s.IsEndFile()) {
      return;
    }
    if (!s.ok()) {
      LOG(WARNING) << "Binlog reader get failed";
      return;
    }
    if (!PikaBinlogTransverter::BinlogItemWithoutContentDecode(TypeFirst, binlog, &item)) {
      LOG(WARNING) << "Binlog item decode failed";
      return;
    }
    // exec_time == 0, could be padding binlog
    if (item.exec_time() != 0) {
      break;
    }
  }

  std::lock_guard l(offset_rwlock_);
  first_offset_.b_offset = offset;
  first_offset_.l_offset.term = item.term_id();
  first_offset_.l_offset.index = item.logic_id();
}

Status StableLog::PurgeFileAfter(uint32_t filenum) {
  std::map<uint32_t, std::string> binlogs;
  bool res = GetBinlogFiles(&binlogs);
  if (!res) {
    return Status::Corruption("GetBinlogFiles failed");
  }
  for (auto& it : binlogs) {
    if (it.first > filenum) {
      // Do delete
      auto filename = log_path_ + it.second;
      if (!pstd::DeleteFile(filename)) {
        return Status::IOError("pstd::DeleteFile faield, filename = " + filename);
      }
      LOG(WARNING) << "Delete file " << filename;
    }
  }
  return Status::OK();
}

Status StableLog::TruncateTo(const LogOffset& offset) {
  Status s = PurgeFileAfter(offset.b_offset.filenum);
  if (!s.ok()) {
    return s;
  }
  return stable_logger_->Truncate(offset.b_offset.filenum, offset.b_offset.offset, offset.l_offset.index);
}
