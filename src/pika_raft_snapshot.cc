// Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_raft_snapshot.h"

#include <fstream>

#include "pstd/include/pstd_defer.h"

#include "include/pika_server.h"
#include "include/pika_conf.h"

using namespace pstd;

extern std::unique_ptr<PikaConf> g_pika_conf;
extern PikaServer* g_pika_server;

SnapshotFileWriter::SnapshotFileWriter(const std::string& filepath) {
  filepath_ = filepath;
  fd_ = open(filepath.c_str(), O_RDWR | O_APPEND | O_CREAT, 0644);
}

Status SnapshotFileWriter::Write(uint64_t offset, size_t n, const char* data) {
  const char* ptr = data;
  size_t left = n;
  Status s;
  while (left != 0) {
    ssize_t done = write(fd_, ptr, left);
    if (done < 0) {
      if (errno == EINTR) {
        continue;
      }
      LOG(WARNING) << "pwrite failed, filename: " << filepath_ << "errno: " << strerror(errno) << "n: " << n;
      return Status::IOError(filepath_, "pwrite failed");
    }
    left -= done;
    ptr += done;
    offset += done;
  }
  return Status::OK();
}

Status SnapshotFileWriter::Close() {
  close(fd_);
  return Status::OK();
}

Status SnapshotFileWriter::Fsync() {
  fsync(fd_);
  return Status::OK();
}

RaftSnapshotCreater::RaftSnapshotCreater() {
  db_structs_ = g_pika_conf->db_structs();
  Clear();
}

void RaftSnapshotCreater::Clear() {
  db_structs_ = g_pika_conf->db_structs();
  db_it_ = db_structs_.begin();
  if (db_it_ == db_structs_.end()) {
    return;
  }
  slot_id_it_ = db_it_->slot_ids.begin();
  if (db_it_->slot_num == 0) {
    return;
  }
  filenames_ = std::vector<std::string>();
  filename_it_ = filenames_.begin();
  offset_ = 0;
  is_eof_ = false;
  done_ = false;
}

bool RaftSnapshotCreater::CreateSnapshotMeta(nuraft::ptr<nuraft::buffer>& snapshot_content) {
  if (done_) return false;
  nuraft::ptr<nuraft::buffer> buf;

  // get meta data information
  uint32_t meta_size = 2*sizeof(uint32_t);
  for(const auto& db : db_structs_) {
    const std::string& db_name = db.db_name;
    meta_size += sizeof(uint32_t) + db_name.size();
    meta_size += 2*sizeof(uint32_t) + 2*db.slot_num*sizeof(uint32_t);
  }

  // put meta data into snapshot
  snapshot_content = nuraft::buffer::alloc(meta_size);
  nuraft::buffer_serializer bs(snapshot_content);
  bs.put_u32(db_structs_.size());
  for(const auto& db : db_structs_) {
    const std::string& db_name = db.db_name;
    bs.put_str(db_name);
    bs.put_u32(db.slot_num);
    for (const auto& slot_id : db.slot_ids) {
      bs.put_u32(slot_id);
    }
  }
  return true;
}

// return true when still having snapshot content to be send
bool RaftSnapshotCreater::CreateNextSnapshotContent(nuraft::ptr<nuraft::buffer>& snapshot_content) {
  if (done_) return false;
  if (db_it_ == db_structs_.end()) return false;
  const std::string& db_name = db_it_->db_name;
  if (slot_id_it_ == db_it_->slot_ids.end()) return false;
  std::string dummy_snapshot_uuid;
  if (filenames_.empty()) {
    g_pika_server->GetDumpMeta(db_name, *slot_id_it_, &filenames_, &dummy_snapshot_uuid);
    filenames_.push_back(kSnapshotInfoName);
    filename_it_ = filenames_.begin();
  }
  if (filename_it_ == filenames_.end()) return false;

  snapshot_content = InternalCreateSnapshot(db_name, *slot_id_it_, *filename_it_);

  // read dump file failed, skip
  if (snapshot_content == nullptr) {
    done_ = true;
    return true;
  }
  // next file
  if (is_eof_) {
    filename_it_ ++;
  }
  // next slot
  if (filename_it_ == filenames_.end()) {
    filenames_.clear();
    slot_id_it_ ++;
  }
  // next db
  if (slot_id_it_ == db_it_->slot_ids.end()) {
    db_it_ ++;
    // skip db with 0 slot
    while (db_it_ != db_structs_.end() && db_it_->slot_num == 0) {
      db_it_ ++;
    }
  }
  // no db to be next, snapshot create done
  if (db_it_ == db_structs_.end()) {
    done_ = true;
    return false;
  } else {
    // first slot of next db
    slot_id_it_ = db_it_->slot_ids.begin();
  }
  return true;
}


nuraft::ptr<nuraft::buffer> RaftSnapshotCreater::InternalCreateSnapshot(const std::string db_name, const uint32_t slot_id, const std::string filename) {
  std::shared_ptr<Slot> slot = g_pika_server->GetDBSlotById(db_name, slot_id);
  if (!slot) {
    LOG(WARNING) << "cannot find slot for db_name: " << db_name
                  << " slot_id: " << slot_id;
    return nullptr;
  }

  Status s;
  const std::string filepath = slot->bgsave_info().path + "/" + filename;
  char* content = new char[fileblock_size_ + 1];
  std::string checksum = "";
  size_t bytes_read{0};
  s = ReadDumpFile(filepath, offset_, fileblock_size_, content, &bytes_read, &checksum);
  if (!s.ok()) {
    delete []content;
    LOG(WARNING) << "read dump file failed: " << db_name
              << " slot_id: " << slot_id
              << " filepath: " << filepath;
    return nullptr;
  }

  // update is_eof_
  is_eof_ = bytes_read != fileblock_size_;

  nuraft::ptr<nuraft::buffer> snapshot_content;
  SerializeSnapshot(content, bytes_read, offset_, is_eof_
            , checksum, filename
            , db_name, slot_id
            , snapshot_content);

  delete []content;

  return snapshot_content;
}

void RaftSnapshotCreater::SerializeSnapshot(char* buffer, size_t bytes_read, size_t offset, bool is_eof
              , std::string checksum, std::string filename
              , std::string db_name, uint32_t slot_id
              , nuraft::ptr<nuraft::buffer>& snapshot_content
              ) {
  size_t content_size = bytes_read + 3*sizeof(uint32_t)
                        + checksum.size() + filename.size()
                        + db_name.size() + sizeof(uint32_t);
  snapshot_content = nuraft::buffer::alloc(8*sizeof(uint32_t) + content_size);
  nuraft::buffer_serializer bs(snapshot_content);
  bs.put_bytes(buffer, bytes_read);
  bs.put_u32(offset);
  bs.put_u32(is_eof);
  bs.put_str(checksum);
  bs.put_str(filename);
  bs.put_str(db_name);
  bs.put_u32(slot_id);
}

void RaftSnapshotCreater::SerializeSnapshotMeta(std::string db_name, uint32_t slot_id
              , std::set<std::string>& filenames
              , nuraft::ptr<nuraft::buffer>& snapshot_content
              ) {
  size_t content_size = db_name.size() + sizeof(uint32_t) + sizeof(uint32_t);
  for (const auto& filename : filenames) {
    content_size += sizeof(uint32_t);
    content_size += filename.size();
  }
  snapshot_content = nuraft::buffer::alloc(3*sizeof(uint32_t) + content_size);
  nuraft::buffer_serializer bs(snapshot_content);
  bs.put_str(db_name);
  bs.put_u32(slot_id);
  bs.put_u32(filenames.size());
  for (const auto& filename : filenames) {
    bs.put_str(filename);
  }
}

Status RaftSnapshotCreater::ReadDumpFile(const std::string filepath, const size_t offset, const size_t count,
                  char* data, size_t* bytes_read, std::string* checksum) {
  int fd = open(filepath.c_str(), O_RDONLY);
  if (fd < 0) {
    return Status::IOError("fd open failed");
  }
  DEFER { close(fd); };

  const int kMaxCopyBlockSize = 1 << 20;
  size_t read_offset = offset;
  size_t read_count = count;
  if (read_count > kMaxCopyBlockSize) {
    read_count = kMaxCopyBlockSize;
  }
  ssize_t bytesin = 0;
  size_t left_read_count = count;

  while ((bytesin = pread(fd, data, read_count, read_offset)) > 0) {
    left_read_count -= bytesin;
    if (left_read_count < 0) {
      break ;
    }
    if (read_count > left_read_count) {
      read_count = left_read_count;
    }

    data += bytesin;
    *bytes_read += bytesin;
    read_offset += bytesin;
  }

  if (bytesin == -1) {
    LOG(ERROR) << "unable to read from " << filepath;
    return pstd::Status::IOError("unable to read from " + filepath);
  }

  if (bytesin == 0) {
    char* buffer = new char[kMaxCopyBlockSize];
    pstd::MD5 md5;

    while ((bytesin = read(fd, buffer, kMaxCopyBlockSize)) > 0) {
      md5.update(buffer, bytesin);
    }
    if (bytesin == -1) {
      LOG(ERROR) << "unable to read from " << filepath;
      delete []buffer;
      return pstd::Status::IOError("unable to read from " + filepath);
    }
    delete []buffer;
    *checksum = md5.finalize().hexdigest();
  }
  return pstd::Status::OK();
}

RaftSnapshotManager::RaftSnapshotManager() {
  std::vector<DBStruct> db_structs = g_pika_conf->db_structs();
  if (db_structs.size() > 0) {
    // snapshot info in each db same for now
    std::string db_name = db_structs[0].db_name;
    LoadLocalSnapshotInfo(g_pika_conf->db_path() + "/" + db_name + "/");
  } else {
    last_snapshot_ = nullptr;
  }

  for(const auto& db : db_structs) {
    const std::string& db_name = db.db_name;
    snapshot_meta_.insert({db_name, db.slot_ids});
  }

  snapshot_creater_ = std::make_unique<RaftSnapshotCreater>();
}

void RaftSnapshotManager::DoBgsave(nuraft::ptr<nuraft::snapshot> ss) {
  std::vector<DBStruct> db_structs = g_pika_conf->db_structs();
  for(const auto& db : db_structs) {
    const std::string& db_name = db.db_name;
    for (const auto& slot_id : db.slot_ids) {
      std::shared_ptr<Slot> slot = g_pika_server->GetDBSlotById(db_name, slot_id);
      slot->BgSaveSlot();
    }
  }
  // wait for bgsave done
  while (g_pika_server->IsBgSaving()) {std::this_thread::sleep_for(std::chrono::milliseconds(500));};
  // save snapshot info to dump per db
  // have to be after bgsave is done
  for(const auto& db : db_structs) {
    const std::string& db_name = db.db_name;
    const std::string db_path = g_pika_conf->db_path() + "/" + db_name;
    std::string db_dump_path = "";
    // every slot in same db share same dump path
    const auto& slot_id_it = db.slot_ids.begin();
    if (slot_id_it != db.slot_ids.end()) {
      std::shared_ptr<Slot> slot = g_pika_server->GetDBSlotById(db_name, *slot_id_it);
      db_dump_path = slot->bgsave_info().path;
    }
    BgsaveSnapshotInfo(db_dump_path, ss);
    // save snapshot info to local db as well
    BgsaveSnapshotInfo(db_path, ss);
  }
}

void RaftSnapshotManager::StartCreateSnapshot() {
  snapshot_creater_->Clear();
}
bool RaftSnapshotManager::CreateSnapshotMeta(nuraft::ptr<nuraft::buffer>& snapshot_content) {
  return snapshot_creater_->CreateSnapshotMeta(snapshot_content);
}
bool RaftSnapshotManager::CreateNextSnapshotContent(nuraft::ptr<nuraft::buffer>& snapshot_content) {
  return snapshot_creater_->CreateNextSnapshotContent(snapshot_content);
}

// dbsync are ready, do ChangeDb
Status RaftSnapshotManager::ApplySnapshot() {
  Status s;
  // update g_pika_conf
  // if sanity check fail just ignore, meaning dbslot already exists
  // only if db does not exist before, AddDB will success and create an empty DB
  for (const auto& db_meta: snapshot_meta_) {
    auto db_name = db_meta.first;
    auto db_slots = db_meta.second;
    s = g_pika_conf->AddDB(db_name, 0);
    s = g_pika_conf->AddDBSlots(db_name, {db_slots});
  }
  // change db
  for(const auto& db : g_pika_conf->db_structs()) {
    const std::string& db_name = db.db_name;
    for (const auto& slot_id : db.slot_ids) {
      const auto& slot = g_pika_server->GetDBSlotById(db_name, slot_id);
      std::string dbsync_path = DbSyncPath(g_pika_conf->db_sync_path(), db_name, slot_id);
      if (!slot->ChangeDb(dbsync_path)) {
        LOG(WARNING) << "Slot: " << db_name << ", Failed to change db";
        s = Status::Corruption("Apply Snapshot Fail When Changing DB");
        return s;
      }
    }
  }
  return Status::OK();
}

void RaftSnapshotManager::InstallSnapshotMeta(nuraft::snapshot& s, nuraft::buffer& snapshot_content) {
  if (snapshot_content.size() == 0) return;
  nuraft::buffer_serializer bs(snapshot_content);
  uint32_t db_count = bs.get_u32();
  for (uint32_t i = 0; i < db_count; i ++) {
    std::string db_name = bs.get_str();
    uint32_t slot_num = bs.get_u32();
    std::set<uint32_t> db_slots;
    for (uint32_t j = 0; j < slot_num; j ++) {
      db_slots.insert(bs.get_u32());
    }
    snapshot_meta_[db_name] = db_slots;
  }
}

void RaftSnapshotManager::PrepareInstallSnapshot() {
  for (const auto& db_slots: snapshot_meta_) {
    const std::string db_name = db_slots.first;
    const auto& slot_ids = db_slots.second;
    for (const auto& slot_id: slot_ids) {
      PrepareInstallSnapshotInternal(db_name, slot_id);
    }
  }
}

Status RaftSnapshotManager::InstallSnapshot(nuraft::buffer& snapshot_content) {
  if (snapshot_content.size() == 0) return Status::OK();
  Status s;
  size_t count = 4 * 1024 * 1024;
  const int kMaxCopyBlockSize = 1 << 20;
  char* content = new char[kMaxCopyBlockSize + 1];
  DEFER {
    delete []content;
  };
  size_t bytes_read;
  std::string db_name_;
  uint32_t slot_id_;
  size_t offset;
  bool is_eof;
  std::string checksum;
  std::string filename;
  std::string db_name;
  uint32_t slot_id;

  DeserializeSnapshot(snapshot_content, content, bytes_read, offset, is_eof, checksum, filename, db_name, slot_id);

  std::string db_sync_path = g_pika_conf->db_sync_path();
  std::string filepath = db_sync_path + (db_sync_path.back() == '/' ? "" : "/") 
                        + db_name + "/" + filename;

  pstd::MD5 md5;
  {
    std::unique_ptr<SnapshotFileWriter> writer(new SnapshotFileWriter(filepath));
    s = writer->Write((uint64_t)offset, bytes_read, content);
    if (!s.ok()) {
      LOG(WARNING) << "rsync client write file error";
    }
    if (writer) {
      s = writer->Fsync();
      if (!s.ok()) {
        s = writer->Close();
        return s;
      }
      s = writer->Close();
      if (!s.ok()) {
          return s;
      }
      writer.reset();
    }
    if (!s.ok()) {
      pstd::DeleteFile(filepath);
    }
  }
  
  offset += bytes_read;

  if (is_eof) {
    if (offset != 0) {
      int fd = open(filepath.c_str(), O_RDONLY);
      if (fd < 0) {
        return Status::IOError("fd open failed");
      }
      DEFER { close(fd); };

      ssize_t bytesin = 0;
      char* buffer = new char[kMaxCopyBlockSize];

      while ((bytesin = read(fd, buffer, kMaxCopyBlockSize)) > 0) {
        md5.update(buffer, bytesin);
      }
      if (bytesin == -1) {
        LOG(ERROR) << "unable to read from " << filepath;
        delete []buffer;
        return pstd::Status::IOError("unable to read from " + filepath);
      }
      delete []buffer;
    } else {
      md5.update(content, bytes_read);
    }

    std::string local_checksum = md5.finalize().hexdigest();

    if (local_checksum != checksum) {
      LOG(WARNING) << "mismatch file checksum for file: " << filename << "\n"
                    << "remote checksum: " << checksum << "\n"
                    << "local checksum: " << local_checksum << "\n";

      s = Status::IOError("mismatch checksum", "mismatch checksum");
      return s;
    }
  }

  return s;
}

void RaftSnapshotManager::DeserializeSnapshot(nuraft::buffer& snapshot_content
                , char* buffer, size_t& bytes_read, size_t& offset, bool& is_eof
                , std::string& checksum, std::string& filename
                , std::string& db_name, uint32_t& slot_id 
                ) {
  nuraft::buffer_serializer bs(snapshot_content);
  void* remote_content = bs.get_bytes(bytes_read);
  std::memcpy(reinterpret_cast<void*>(buffer), remote_content, bytes_read);
  offset = bs.get_u32();
  is_eof = bs.get_u32();
  checksum = bs.get_str();
  filename = bs.get_str();
  db_name = bs.get_str();
  slot_id = bs.get_u32();
}

void RaftSnapshotManager::SetLastSnapshot(nuraft::ptr<nuraft::snapshot> snapshot) {
  last_snapshot_ = std::move(snapshot);
}

nuraft::ptr<nuraft::snapshot> RaftSnapshotManager::GetLastSnapshot() {
  return last_snapshot_;
}

void RaftSnapshotManager::PrepareInstallSnapshotInternal(std::string db_name, uint32_t slot_id) {
  std::string dbsync_path = DbSyncPath(g_pika_conf->db_sync_path(), db_name, slot_id);
  pstd::DeleteDirIfExist(dbsync_path);
  pstd::CreatePath(dbsync_path + "strings");
  pstd::CreatePath(dbsync_path + "hashes");
  pstd::CreatePath(dbsync_path + "lists");
  pstd::CreatePath(dbsync_path + "sets");
  pstd::CreatePath(dbsync_path + "zsets");
}

void RaftSnapshotManager::BgsaveSnapshotInfo(const std::string& db_dump_path, nuraft::ptr<nuraft::snapshot> ss) {
  std::string dump_snapshot_info_path = db_dump_path + "/" + kSnapshotInfoName;
  std::stringstream info_content;
  std::ofstream out;
  // snapshot info in dump
  out.open(dump_snapshot_info_path, std::ios::in | std::ios::trunc);
  if (out.is_open()) {
    if (ss) {
      info_content << kRaftIdxPrefix << ss->get_last_log_idx() << "\n"
                    << kRaftTermPrefix << ss->get_last_log_term() << "\n";
      out << info_content.rdbuf();
    }
    out.close();
  }
}

Status RaftSnapshotManager::LoadLocalSnapshotInfo(const std::string& db_path) {
  std::string snpinfo_file_path = db_path + "/" + kSnapshotInfoName;
  ulong snapshot_idx = 0;
  ulong snapshot_term = 0;
  if (!pstd::FileExists(snpinfo_file_path)) {
    last_snapshot_ = nullptr;
    return Status::OK();
  }

  FILE* fp;
  char* line = nullptr;
  size_t len = 0;
  size_t read = 0;
  int32_t line_num = 0;

  std::atomic_int8_t retry_times = 5;

  while (retry_times-- > 0) {
    fp = fopen(snpinfo_file_path.c_str(), "r");
    if (fp == nullptr) {
      LOG(WARNING) << "open snapshot info file failed, path: " << db_path;
    } else {
      break;
    }
  }

  // if the file cannot be read from disk, use the remote file directly
  if (fp == nullptr) {
    LOG(WARNING) << "open snapshot info file failed, path: " << snpinfo_file_path << ", retry times: " << retry_times;
    return Status::IOError("open snapshot info file failed, dir: ", snpinfo_file_path);
  }
  while ((read = getline(&line, &len, fp)) != -1) {
    std::string str(line);
    std::string::size_type pos;
    while ((pos = str.find("\r")) != std::string::npos) {
      str.erase(pos, 1);
    }
    while ((pos = str.find("\n")) != std::string::npos) {
      str.erase(pos, 1);
    }

    if (str.empty()) {
      continue;
    }

    switch (line_num) {
      case 0: {
        snapshot_idx = std::stoul(str.erase(0, kRaftIdxPrefix.size()));
        break;
      }
      case 1: {
        snapshot_term = std::stoul(str.erase(0, kRaftTermPrefix.size()));
        break;
      }
      default: break;
    }

    line_num++;
  }
  fclose(fp);

  last_snapshot_ = nuraft::cs_new<nuraft::snapshot>(snapshot_idx, snapshot_term, nuraft::cs_new<nuraft::cluster_config>());

  return Status::OK();
}

std::string RaftSnapshotManager::DbSyncPath(const std::string& sync_path, const std::string& db_name, const uint32_t slot_id) {
  char buf[256];
  std::string slot_id_str = std::to_string(slot_id);
  snprintf(buf, sizeof(buf), "%s/", db_name.data());
  return sync_path + buf;
}
