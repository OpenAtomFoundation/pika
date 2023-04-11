// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_binlog.h"

#include <fcntl.h>
#include <glog/logging.h>
#include <sys/time.h>

#include "include/pika_binlog_transverter.h"

using pstd::RWLock;

std::string NewFileName(const std::string name, const uint32_t current) {
  char buf[256];
  snprintf(buf, sizeof(buf), "%s%u", name.c_str(), current);
  return std::string(buf);
}

/*
 * Version
 */
Version::Version(pstd::RWFile* save) : pro_num_(0), pro_offset_(0), logic_id_(0), save_(save) {
  assert(save_ != NULL);

  pthread_rwlock_init(&rwlock_, NULL);
}

Version::~Version() {
  StableSave();
  pthread_rwlock_destroy(&rwlock_);
}

Status Version::StableSave() {
  char* p = save_->GetData();
  memcpy(p, &pro_num_, sizeof(uint32_t));
  p += 4;
  memcpy(p, &pro_offset_, sizeof(uint64_t));
  p += 8;
  memcpy(p, &logic_id_, sizeof(uint64_t));
  p += 8;
  memcpy(p, &term_, sizeof(uint32_t));
  return Status::OK();
}

Status Version::Init() {
  Status s;
  if (save_->GetData() != NULL) {
    memcpy((char*)(&pro_num_), save_->GetData(), sizeof(uint32_t));
    memcpy((char*)(&pro_offset_), save_->GetData() + 4, sizeof(uint64_t));
    memcpy((char*)(&logic_id_), save_->GetData() + 12, sizeof(uint64_t));
    memcpy((char*)(&term_), save_->GetData() + 20, sizeof(uint32_t));
    return Status::OK();
  } else {
    return Status::Corruption("version init error");
  }
}

/*
 * Binlog
 */
Binlog::Binlog(const std::string& binlog_path, const int file_size)
    : opened_(false),
      version_(NULL),
      queue_(NULL),
      versionfile_(NULL),
      pro_num_(0),
      pool_(NULL),
      exit_all_consume_(false),
      binlog_path_(binlog_path),
      file_size_(file_size),
      binlog_io_error_(false) {
  // To intergrate with old version, we don't set mmap file size to 100M;
  // pstd::SetMmapBoundSize(file_size);
  // pstd::kMmapBoundSize = 1024 * 1024 * 100;

  Status s;

  pstd::CreateDir(binlog_path_);

  filename_ = binlog_path_ + kBinlogPrefix;
  const std::string manifest = binlog_path_ + kManifest;
  std::string profile;

  if (!pstd::FileExists(manifest)) {
    LOG(INFO) << "Binlog: Manifest file not exist, we create a new one.";

    profile = NewFileName(filename_, pro_num_);
    s = pstd::NewWritableFile(profile, &queue_);
    if (!s.ok()) {
      LOG(FATAL) << "Binlog: new " << filename_ << " " << s.ToString();
    }

    s = pstd::NewRWFile(manifest, &versionfile_);
    if (!s.ok()) {
      LOG(FATAL) << "Binlog: new versionfile error " << s.ToString();
    }

    version_ = new Version(versionfile_);
    version_->StableSave();
  } else {
    LOG(INFO) << "Binlog: Find the exist file.";

    s = pstd::NewRWFile(manifest, &versionfile_);
    if (s.ok()) {
      version_ = new Version(versionfile_);
      version_->Init();
      pro_num_ = version_->pro_num_;

      // Debug
      // version_->debug();
    } else {
      LOG(FATAL) << "Binlog: open versionfile error";
    }

    profile = NewFileName(filename_, pro_num_);
    DLOG(INFO) << "Binlog: open profile " << profile;
    s = pstd::AppendWritableFile(profile, &queue_, version_->pro_offset_);
    if (!s.ok()) {
      LOG(FATAL) << "Binlog: Open file " << profile << " error " << s.ToString();
    }

    uint64_t filesize = queue_->Filesize();
    DLOG(INFO) << "Binlog: filesize is " << filesize;
  }

  InitLogFile();
}

Binlog::~Binlog() {
  pstd::MutexLock l(&mutex_);
  Close();
  delete version_;
  delete versionfile_;

  delete queue_;
}

void Binlog::Close() {
  if (!opened_.load()) {
    return;
  }
  opened_.store(false);
}

void Binlog::InitLogFile() {
  assert(queue_ != NULL);

  uint64_t filesize = queue_->Filesize();
  block_offset_ = filesize % kBlockSize;

  opened_.store(true);
}

Status Binlog::GetProducerStatus(uint32_t* filenum, uint64_t* pro_offset, uint32_t* term, uint64_t* logic_id) {
  if (!opened_.load()) {
    return Status::Busy("Binlog is not open yet");
  }

  pstd::RWLock l(&(version_->rwlock_), false);

  *filenum = version_->pro_num_;
  *pro_offset = version_->pro_offset_;
  if (logic_id != NULL) {
    *logic_id = version_->logic_id_;
  }
  if (term != NULL) {
    *term = version_->term_;
  }

  return Status::OK();
}

// Note: mutex lock should be held
Status Binlog::Put(const std::string& item) {
  if (!opened_.load()) {
    return Status::Busy("Binlog is not open yet");
  }
  Status s = Put(item.c_str(), item.size());
  if (!s.ok()) {
    binlog_io_error_.store(true);
  }
  return s;
}

// Note: mutex lock should be held
Status Binlog::Put(const char* item, int len) {
  Status s;

  /* Check to roll log file */
  uint64_t filesize = queue_->Filesize();
  if (filesize > file_size_) {
    pstd::WritableFile* queue = nullptr;
    std::string profile = NewFileName(filename_, pro_num_ + 1);
    s = pstd::NewWritableFile(profile, &queue);
    if (!s.ok()) {
      LOG(ERROR) << "Binlog: new " << filename_ << " " << s.ToString();
      return s;
    }
    delete queue_;
    queue_ = queue;
    pro_num_++;

    {
      pstd::RWLock l(&(version_->rwlock_), true);
      version_->pro_offset_ = 0;
      version_->pro_num_ = pro_num_;
      version_->StableSave();
    }
    InitLogFile();
  }

  int pro_offset;
  s = Produce(Slice(item, len), &pro_offset);
  if (s.ok()) {
    pstd::RWLock l(&(version_->rwlock_), true);
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

Status Binlog::AppendPadding(pstd::WritableFile* file, uint64_t* len) {
  if (*len < kHeaderSize) {
    return Status::OK();
  }

  Status s;
  char buf[kBlockSize];
  uint64_t now;
  struct timeval tv;
  gettimeofday(&tv, NULL);
  now = tv.tv_sec;

  uint64_t left = *len;
  while (left > 0 && s.ok()) {
    uint32_t size = (left >= kBlockSize) ? kBlockSize : left;
    if (size < kHeaderSize) {
      break;
    } else {
      uint32_t bsize = size - kHeaderSize;
      std::string binlog(bsize, '*');
      buf[0] = static_cast<char>(bsize & 0xff);
      buf[1] = static_cast<char>((bsize & 0xff00) >> 8);
      buf[2] = static_cast<char>(bsize >> 16);
      buf[3] = static_cast<char>(now & 0xff);
      buf[4] = static_cast<char>((now & 0xff00) >> 8);
      buf[5] = static_cast<char>((now & 0xff0000) >> 16);
      buf[6] = static_cast<char>((now & 0xff000000) >> 24);
      // kBadRecord here
      buf[7] = static_cast<char>(kBadRecord);
      s = file->Append(Slice(buf, kHeaderSize));
      if (s.ok()) {
        s = file->Append(Slice(binlog.data(), binlog.size()));
        if (s.ok()) {
          s = file->Flush();
          left -= size;
        }
      }
    }
  }
  *len -= left;
  if (left != 0) {
    LOG(WARNING) << "AppendPadding left bytes: " << left << " is less then kHeaderSize";
  }
  return s;
}

Status Binlog::SetProducerStatus(uint32_t pro_num, uint64_t pro_offset, uint32_t term, uint64_t index) {
  if (!opened_.load()) {
    return Status::Busy("Binlog is not open yet");
  }

  pstd::MutexLock l(&mutex_);

  // offset smaller than the first header
  if (pro_offset < 4) {
    pro_offset = 0;
  }

  delete queue_;

  std::string init_profile = NewFileName(filename_, 0);
  if (pstd::FileExists(init_profile)) {
    pstd::DeleteFile(init_profile);
  }

  std::string profile = NewFileName(filename_, pro_num);
  if (pstd::FileExists(profile)) {
    pstd::DeleteFile(profile);
  }

  pstd::NewWritableFile(profile, &queue_);
  Binlog::AppendPadding(queue_, &pro_offset);

  pro_num_ = pro_num;

  {
    pstd::RWLock l(&(version_->rwlock_), true);
    version_->pro_num_ = pro_num;
    version_->pro_offset_ = pro_offset;
    version_->term_ = term;
    version_->logic_id_ = index;
    version_->StableSave();
  }

  InitLogFile();
  return Status::OK();
}

Status Binlog::Truncate(uint32_t pro_num, uint64_t pro_offset, uint64_t index) {
  delete queue_;
  std::string profile = NewFileName(filename_, pro_num);
  const int fd = open(profile.c_str(), O_RDWR | O_CLOEXEC, 0644);
  if (fd < 0) {
    return Status::IOError("fd open failed");
  }
  if (ftruncate(fd, pro_offset)) {
    return Status::IOError("ftruncate failed");
  }
  close(fd);

  pro_num_ = pro_num;
  {
    pstd::RWLock l(&(version_->rwlock_), true);
    version_->pro_num_ = pro_num;
    version_->pro_offset_ = pro_offset;
    version_->logic_id_ = index;
    version_->StableSave();
  }

  Status s = pstd::AppendWritableFile(profile, &queue_, version_->pro_offset_);
  if (!s.ok()) {
    return s;
  }

  InitLogFile();

  return Status::OK();
}
