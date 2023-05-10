// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <iostream>

#include "include/pika_binlog.h"

#include <sys/time.h>

std::string NewFileName(const std::string name, const uint32_t current) {
  char buf[256];
  snprintf(buf, sizeof(buf), "%s%u", name.c_str(), current);
  return std::string(buf);
}

/*
 * Version
 */
Version::Version(pstd::RWFile* save) : pro_num_(0), pro_offset_(0), logic_id_(0), save_(save) { assert(save_ != NULL); }

Version::~Version() { StableSave(); }

Status Version::StableSave() {
  char* p = save_->GetData();
  memcpy(p, &pro_num_, sizeof(uint32_t));
  p += 4;
  memcpy(p, &pro_offset_, sizeof(uint64_t));
  p += 8;
  memcpy(p, &logic_id_, sizeof(uint64_t));
  p += 8;
  return Status::OK();
}

Status Version::Init() {
  Status s;
  if (save_->GetData() != NULL) {
    memcpy((char*)(&pro_num_), save_->GetData(), sizeof(uint32_t));
    memcpy((char*)(&pro_offset_), save_->GetData() + 4, sizeof(uint64_t));
    memcpy((char*)(&logic_id_), save_->GetData() + 12, sizeof(uint64_t));
    return Status::OK();
  } else {
    return Status::Corruption("version init error");
  }
}

/*
 * Binlog
 */
Binlog::Binlog(const std::string& binlog_path, const int file_size)
    : consumer_num_(0),
      version_(NULL),
      queue_(NULL),
      versionfile_(NULL),
      pro_num_(0),
      pool_(NULL),
      exit_all_consume_(false),
      binlog_path_(binlog_path),
      file_size_(file_size) {
  // To intergrate with old version, we don't set mmap file size to 100M;
  // pstd::SetMmapBoundSize(file_size);
  // pstd::kMmapBoundSize = 1024 * 1024 * 100;

  Status s;

  pstd::CreateDir(binlog_path_);

  filename = binlog_path_ + kBinlogPrefix;
  const std::string manifest = binlog_path_ + kManifest;
  std::string profile;

  if (!pstd::FileExists(manifest)) {
    std::cout << "Binlog: Manifest file not exist, we create a new one.";

    profile = NewFileName(filename, pro_num_);
    s = pstd::NewWritableFile(profile, &queue_);
    if (!s.ok()) {
      std::cout << "Binlog: new " << filename << " " << s.ToString();
      exit(-1);
    }

    s = pstd::NewRWFile(manifest, &versionfile_);
    if (!s.ok()) {
      std::cout << "Binlog: new versionfile error " << s.ToString();
      exit(-1);
    }

    version_ = new Version(versionfile_);
    version_->StableSave();
  } else {
    std::cout << "Binlog: Find the exist file.";

    s = pstd::NewRWFile(manifest, &versionfile_);
    if (s.ok()) {
      version_ = new Version(versionfile_);
      version_->Init();
      pro_num_ = version_->pro_num_;

      // Debug
      // version_->debug();
    } else {
      std::cout << "Binlog: open versionfile error";
      exit(-1);
    }

    profile = NewFileName(filename, pro_num_);
    s = pstd::AppendWritableFile(profile, &queue_, version_->pro_offset_);
    if (!s.ok()) {
      std::cout << "Binlog: Open file " << profile << " error " << s.ToString();
      exit(-1);
    }
  }

  InitLogFile();
}

Binlog::~Binlog() {
  delete version_;
  delete versionfile_;

  delete queue_;
}

void Binlog::InitLogFile() {
  assert(queue_ != NULL);

  uint64_t filesize = queue_->Filesize();
  block_offset_ = filesize % kBlockSize;
}

Status Binlog::GetProducerStatus(uint32_t* filenum, uint64_t* pro_offset, uint64_t* logic_id) {
  std::shared_lock l(version_->rwlock_);

  *filenum = version_->pro_num_;
  *pro_offset = version_->pro_offset_;
  if (logic_id != NULL) {
    *logic_id = version_->logic_id_;
  }

  return Status::OK();
}

// Note: mutex lock should be held
Status Binlog::Put(const std::string& item) { return Put(item.c_str(), item.size()); }

// Note: mutex lock should be held
Status Binlog::Put(const char* item, int len) {
  Status s;

  /* Check to roll log file */
  uint64_t filesize = queue_->Filesize();
  if (filesize > file_size_) {
    delete queue_;
    queue_ = NULL;

    pro_num_++;
    std::string profile = NewFileName(filename, pro_num_);
    pstd::NewWritableFile(profile, &queue_);

    {
      std::lock_guard l(version_->rwlock_);
      version_->pro_offset_ = 0;
      version_->pro_num_ = pro_num_;
      version_->StableSave();
    }
    InitLogFile();
  }

  int pro_offset;
  s = Produce(Slice(item, len), &pro_offset);
  if (s.ok()) {
    std::lock_guard l(version_->rwlock_);
    version_->pro_offset_ = pro_offset;
    version_->logic_id_++;
    version_->StableSave();
  }

  return s;
}

Status Binlog::EmitPhysicalRecord(RecordType t, const char* ptr, size_t n, int* temp_pro_offset) {
  Status s;
  assert(n <= 0xffffff);
  assert(block_offset_ + kHeaderSize + n <= kBlockSize);

  char buf[kHeaderSize];

  uint64_t now;
  struct timeval tv;
  gettimeofday(&tv, NULL);
  now = tv.tv_sec;
  buf[0] = static_cast<char>(n & 0xff);
  buf[1] = static_cast<char>((n & 0xff00) >> 8);
  buf[2] = static_cast<char>(n >> 16);
  buf[3] = static_cast<char>(now & 0xff);
  buf[4] = static_cast<char>((now & 0xff00) >> 8);
  buf[5] = static_cast<char>((now & 0xff0000) >> 16);
  buf[6] = static_cast<char>((now & 0xff000000) >> 24);
  buf[7] = static_cast<char>(t);

  s = queue_->Append(Slice(buf, kHeaderSize));
  if (s.ok()) {
    s = queue_->Append(Slice(ptr, n));
    if (s.ok()) {
      s = queue_->Flush();
    }
  }
  block_offset_ += static_cast<int>(kHeaderSize + n);

  *temp_pro_offset += kHeaderSize + n;
  return s;
}

Status Binlog::Produce(const Slice& item, int* temp_pro_offset) {
  Status s;
  const char* ptr = item.data();
  size_t left = item.size();
  bool begin = true;

  *temp_pro_offset = version_->pro_offset_;
  do {
    const int leftover = static_cast<int>(kBlockSize) - block_offset_;
    assert(leftover >= 0);
    if (static_cast<size_t>(leftover) < kHeaderSize) {
      if (leftover > 0) {
        s = queue_->Append(Slice("\x00\x00\x00\x00\x00\x00\x00", leftover));
        if (!s.ok()) {
          return s;
        }
        *temp_pro_offset += leftover;
      }
      block_offset_ = 0;
    }

    const size_t avail = kBlockSize - block_offset_ - kHeaderSize;
    const size_t fragment_length = (left < avail) ? left : avail;
    RecordType type;
    const bool end = (left == fragment_length);
    if (begin && end) {
      type = kFullType;
    } else if (begin) {
      type = kFirstType;
    } else if (end) {
      type = kLastType;
    } else {
      type = kMiddleType;
    }

    s = EmitPhysicalRecord(type, ptr, fragment_length, temp_pro_offset);
    ptr += fragment_length;
    left -= fragment_length;
    begin = false;
  } while (s.ok() && left > 0);

  return s;
}

Status Binlog::AppendBlank(pstd::WritableFile* file, uint64_t len) {
  if (len < kHeaderSize) {
    return Status::OK();
  }

  uint64_t pos = 0;

  std::string blank(kBlockSize, ' ');
  for (; pos + kBlockSize < len; pos += kBlockSize) {
    file->Append(Slice(blank.data(), blank.size()));
  }

  // Append a msg which occupy the remain part of the last block
  // We simply increase the remain length to kHeaderSize when remain part < kHeaderSize
  uint32_t n;
  if (len % kBlockSize < kHeaderSize) {
    n = 0;
  } else {
    n = (uint32_t)((len % kBlockSize) - kHeaderSize);
  }

  char buf[kBlockSize];
  uint64_t now;
  struct timeval tv;
  gettimeofday(&tv, NULL);
  now = tv.tv_sec;
  buf[0] = static_cast<char>(n & 0xff);
  buf[1] = static_cast<char>((n & 0xff00) >> 8);
  buf[2] = static_cast<char>(n >> 16);
  buf[3] = static_cast<char>(now & 0xff);
  buf[4] = static_cast<char>((now & 0xff00) >> 8);
  buf[5] = static_cast<char>((now & 0xff0000) >> 16);
  buf[6] = static_cast<char>((now & 0xff000000) >> 24);
  buf[7] = static_cast<char>(kFullType);

  Status s = file->Append(Slice(buf, kHeaderSize));
  if (s.ok()) {
    s = file->Append(Slice(blank.data(), n));
    if (s.ok()) {
      s = file->Flush();
    }
  }
  return s;
}

Status Binlog::SetProducerStatus(uint32_t pro_num, uint64_t pro_offset) {
  std::lock_guard l(mutex_);

  // offset smaller than the first header
  if (pro_offset < 4) {
    pro_offset = 0;
  }

  delete queue_;

  std::string init_profile = NewFileName(filename, 0);
  if (pstd::FileExists(init_profile)) {
    pstd::DeleteFile(init_profile);
  }

  std::string profile = NewFileName(filename, pro_num);
  if (pstd::FileExists(profile)) {
    pstd::DeleteFile(profile);
  }

  pstd::NewWritableFile(profile, &queue_);
  Binlog::AppendBlank(queue_, pro_offset);

  pro_num_ = pro_num;

  {
    std::lock_guard l(version_->rwlock_);
    version_->pro_num_ = pro_num;
    version_->pro_offset_ = pro_offset;
    version_->StableSave();
  }

  InitLogFile();
  return Status::OK();
}
