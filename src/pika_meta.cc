// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_meta.h"
#include "pika_inner_message.pb.h"

using pstd::Status;

const uint32_t VERSION = 1;

void PikaMeta::SetPath(const std::string& path) { local_meta_path_ = path; }

/*
 * ******************* Meta File Format ******************
 * |   <Version>   |   <Meta Size>   |      <Meta>      |
 *      4 Bytes          4 Bytes        meta size Bytes
 */
Status PikaMeta::StableSave(const std::vector<DBStruct>& db_structs) {
  std::lock_guard l(rwlock_);
  if (local_meta_path_.empty()) {
    LOG(WARNING) << "Local meta file path empty";
    return Status::Corruption("local meta file path empty");
  }
  std::string local_meta_file = local_meta_path_ + kPikaMeta;
  std::string tmp_file = local_meta_file;
  tmp_file.append("_tmp");

  std::unique_ptr<pstd::RWFile> saver;
  pstd::CreatePath(local_meta_path_);
  Status s = pstd::NewRWFile(tmp_file, saver);
  if (!s.ok()) {
    LOG(WARNING) << "Open local meta file failed";
    return Status::Corruption("open local meta file failed");
  }

  InnerMessage::PikaMeta meta;
  for (const auto& ts : db_structs) {
    InnerMessage::DBInfo* db_info = meta.add_db_infos();
    db_info->set_db_name(ts.db_name);
    db_info->set_slot_num(ts.slot_num);
    for (const auto& id : ts.slot_ids) {
      db_info->add_slot_ids(id);
    }
  }

  std::string meta_str;
  if (!meta.SerializeToString(&meta_str)) {
    LOG(WARNING) << "Serialize meta string failed";
    return Status::Corruption("serialize meta string failed");
  }
  uint32_t meta_str_size = meta_str.size();

  char* p = saver->GetData();
  memcpy(p, &VERSION, sizeof(uint32_t));
  p += sizeof(uint32_t);
  memcpy(p, &meta_str_size, sizeof(uint32_t));
  p += sizeof(uint32_t);
  strncpy(p, meta_str.data(), meta_str.size());

  pstd::DeleteFile(local_meta_file);
  if (pstd::RenameFile(tmp_file, local_meta_file) != 0) {
    LOG(WARNING) << "Failed to rename file, error: " << strerror(errno);
    return Status::Corruption("faild to rename file");
  }
  return Status::OK();
}

Status PikaMeta::ParseMeta(std::vector<DBStruct>* const db_structs) {
  std::shared_lock l(rwlock_);
  std::string local_meta_file = local_meta_path_ + kPikaMeta;
  if (!pstd::FileExists(local_meta_file)) {
    LOG(WARNING) << "Local meta file not found, path: " << local_meta_file;
    return Status::Corruption("meta file not found");
  }

  std::unique_ptr<pstd::RWFile> reader;
  Status s = pstd::NewRWFile(local_meta_file, reader);
  if (!s.ok()) {
    LOG(WARNING) << "Open local meta file failed";
    return Status::Corruption("open local meta file failed");
  }

  if (!reader->GetData()) {
    LOG(WARNING) << "Meta file init error";
    return Status::Corruption("meta file init error");
  }

  uint32_t version = 0;
  uint32_t meta_size = 0;
  memcpy(reinterpret_cast<char*>(&version), reader->GetData(), sizeof(uint32_t));
  memcpy(reinterpret_cast<char*>(&meta_size), reader->GetData() + sizeof(uint32_t), sizeof(uint32_t));
  auto const buf_ptr = std::make_unique<char[]>(meta_size);
  char* const buf = buf_ptr.get();
  memcpy(buf, reader->GetData() + 2 * sizeof(uint32_t), meta_size);

  InnerMessage::PikaMeta meta;
  if (!meta.ParseFromArray(buf, static_cast<int32_t>(meta_size))) {
    LOG(WARNING) << "Parse meta string failed";
    return Status::Corruption("parse meta string failed");
  }

  db_structs->clear();
  for (int idx = 0; idx < meta.db_infos_size(); ++idx) {
    const InnerMessage::DBInfo& ti = meta.db_infos(idx);
    std::set<uint32_t> slot_ids;
    for (int sidx = 0; sidx < ti.slot_ids_size(); ++sidx) {
      slot_ids.insert(ti.slot_ids(sidx));
    }
    db_structs->emplace_back(ti.db_name(), ti.slot_num(), slot_ids);
  }
  return Status::OK();
}
