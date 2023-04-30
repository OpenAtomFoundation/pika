// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_meta.h"
#include "pika_inner_message.pb.h"

const uint32_t VERSION = 1;

PikaMeta::PikaMeta() : local_meta_path_("") { pthread_rwlock_init(&rwlock_, nullptr); }

PikaMeta::~PikaMeta() { pthread_rwlock_destroy(&rwlock_); }

void PikaMeta::SetPath(const std::string& path) { local_meta_path_ = path; }

/*
 * ******************* Meta File Format ******************
 * |   <Version>   |   <Meta Size>   |      <Meta>      |
 *      4 Bytes          4 Bytes        meta size Bytes
 */
Status PikaMeta::StableSave(const std::vector<TableStruct>& table_structs) {
  pstd::RWLock l(&rwlock_, true);
  if (local_meta_path_.empty()) {
    LOG(WARNING) << "Local meta file path empty";
    return Status::Corruption("local meta file path empty");
  }
  std::string local_meta_file = local_meta_path_ + kPikaMeta;
  std::string tmp_file = local_meta_file;
  tmp_file.append("_tmp");

  pstd::RWFile* saver = nullptr;
  pstd::CreatePath(local_meta_path_);
  Status s = pstd::NewRWFile(tmp_file, &saver);
  if (!s.ok()) {
    delete saver;
    LOG(WARNING) << "Open local meta file failed";
    return Status::Corruption("open local meta file failed");
  }

  InnerMessage::PikaMeta meta;
  for (const auto& ts : table_structs) {
    InnerMessage::TableInfo* table_info = meta.add_table_infos();
    table_info->set_table_name(ts.table_name);
    table_info->set_partition_num(ts.partition_num);
    for (const auto& id : ts.partition_ids) {
      table_info->add_partition_ids(id);
    }
  }

  std::string meta_str;
  if (!meta.SerializeToString(&meta_str)) {
    delete saver;
    LOG(WARNING) << "Serialize meta string failed";
    return Status::Corruption("serialize meta string failed");
  }
  uint32_t meta_str_size = meta_str.size();

  char* p = saver->GetData();
  memcpy(p, &VERSION, sizeof(uint32_t));
  p += sizeof(uint32_t);
  memcpy(p, &meta_str_size, sizeof(uint32_t));
  p += sizeof(uint32_t);
  memcpy(p, meta_str.data(), meta_str.size());
  delete saver;

  pstd::DeleteFile(local_meta_file);
  if (pstd::RenameFile(tmp_file, local_meta_file)) {
    LOG(WARNING) << "Failed to rename file, error: " << strerror(errno);
    return Status::Corruption("faild to rename file");
  }
  return Status::OK();
}

Status PikaMeta::ParseMeta(std::vector<TableStruct>* const table_structs) {
  pstd::RWLock l(&rwlock_, false);
  std::string local_meta_file = local_meta_path_ + kPikaMeta;
  if (!pstd::FileExists(local_meta_file)) {
    LOG(WARNING) << "Local meta file not found, path: " << local_meta_file;
    return Status::Corruption("meta file not found");
  }

  pstd::RWFile* reader = nullptr;
  Status s = pstd::NewRWFile(local_meta_file, &reader);
  if (!s.ok()) {
    delete reader;
    LOG(WARNING) << "Open local meta file failed";
    return Status::Corruption("open local meta file failed");
  }

  if (reader->GetData() == nullptr) {
    delete reader;
    LOG(WARNING) << "Meta file init error";
    return Status::Corruption("meta file init error");
  }

  uint32_t version = 0;
  uint32_t meta_size = 0;
  memcpy((char*)(&version), reader->GetData(), sizeof(uint32_t));
  memcpy((char*)(&meta_size), reader->GetData() + sizeof(uint32_t), sizeof(uint32_t));
  char* const buf = new char[meta_size];
  memcpy(buf, reader->GetData() + 2 * sizeof(uint32_t), meta_size);

  InnerMessage::PikaMeta meta;
  if (!meta.ParseFromArray(buf, meta_size)) {
    delete[] buf;
    delete reader;
    LOG(WARNING) << "Parse meta string failed";
    return Status::Corruption("parse meta string failed");
  }
  delete[] buf;
  delete reader;

  table_structs->clear();
  for (int idx = 0; idx < meta.table_infos_size(); ++idx) {
    InnerMessage::TableInfo ti = meta.table_infos(idx);
    std::set<uint32_t> partition_ids;
    for (int sidx = 0; sidx < ti.partition_ids_size(); ++sidx) {
      partition_ids.insert(ti.partition_ids(sidx));
    }
    table_structs->emplace_back(ti.table_name(), ti.partition_num(), partition_ids);
  }
  return Status::OK();
}
