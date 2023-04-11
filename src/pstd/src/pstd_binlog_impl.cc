// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "pstd/src/pstd_binlog_impl.h"

#include <assert.h>
#include <stddef.h>
#include <sys/time.h>
#include <unistd.h>
#include <string>

namespace pstd {

std::string NewFileName(const std::string name, const uint32_t current) {
  char buf[256];
  snprintf(buf, sizeof(buf), "%s%u", name.c_str(), current);
  return std::string(buf);
}

// Version
Version::Version(RWFile* save) : pro_offset_(0), pro_num_(0), item_num_(0), save_(save) { assert(save_ != NULL); }

Version::~Version() { StableSave(); }

Status Version::StableSave() {
  char* p = save_->GetData();
  memcpy(p, &pro_offset_, sizeof(uint64_t));
  p += 16;
  memcpy(p, &item_num_, sizeof(uint32_t));
  p += 4;
  memcpy(p, &pro_num_, sizeof(uint32_t));
  return Status::OK();
}

Status Version::Init() {
  Status s;
  if (save_->GetData() != NULL) {
    memcpy((char*)(&pro_offset_), save_->GetData(), sizeof(uint64_t));
    memcpy((char*)(&item_num_), save_->GetData() + 16, sizeof(uint32_t));
    memcpy((char*)(&pro_num_), save_->GetData() + 20, sizeof(uint32_t));
    return Status::OK();
  } else {
    return Status::Corruption("version init error");
  }
}

// Binlog
Status Binlog::Open(const std::string& path, Binlog** logptr) {
  *logptr = NULL;

  BinlogImpl* impl = new BinlogImpl(path, kBinlogSize);
  Status s = impl->Recover();
  if (s.ok()) {
    *logptr = impl;
  } else {
    delete impl;
  }
  return s;
}

BinlogImpl::BinlogImpl(const std::string& path, const int file_size)
    : exit_all_consume_(false), path_(path), file_size_(file_size), version_(NULL), queue_(NULL), versionfile_(NULL) {
  if (path_.back() != '/') {
    path_.push_back('/');
  }
}

Status BinlogImpl::Recover() {
  CreateDir(path_);

  std::string manifest = path_ + kManifest;
  bool exist_flag = false;
  if (FileExists(manifest)) {
    exist_flag = true;
  }
  Status s = NewRWFile(manifest, &versionfile_);
  if (!s.ok()) {
    return s;
  }
  version_ = new Version(versionfile_);
  version_->Init();
  version_->StableSave();

  pro_num_ = version_->pro_num_;
  std::string profile = NewFileName(path_ + kBinlogPrefix, pro_num_);
  if (exist_flag) {
    s = AppendWritableFile(profile, &queue_, version_->pro_offset_);
    if (!s.ok()) {
      return s;
    }

    // recover memtable
    // MemTable *mem = new MemTable(file_size_);
    // mem->Ref();
    // mem->RecoverFromFile(profile);
    // memtables_[pro_num_] = mem;
  } else {
    s = NewWritableFile(profile, &queue_);
    if (!s.ok()) {
      return s;
    }

    // MemTable *mem = new MemTable(file_size_);
    // mem->Ref();
    // memtables_[pro_num_] = mem;
  }

  InitOffset();
  return s;
}

BinlogImpl::~BinlogImpl() {
  delete version_;
  delete versionfile_;
  delete queue_;
}

void BinlogImpl::InitOffset() {
  assert(queue_ != NULL);
  uint64_t filesize = queue_->Filesize();
  block_offset_ = filesize % kBlockSize;
}

Status BinlogImpl::GetProducerStatus(uint32_t* filenum, uint64_t* offset) {
  ReadLock(&version_->rwlock_);
  *filenum = version_->pro_num_;
  *offset = version_->pro_offset_;
  return Status::OK();
}

// Note: mutex lock should be held
Status BinlogImpl::Append(const std::string& item) {
  Status s;

  // Check to roll log file
  uint64_t filesize = queue_->Filesize();
  if (filesize > file_size_) {
    delete queue_;
    queue_ = NULL;

    pro_num_++;
    std::string profile = NewFileName(path_ + kBinlogPrefix, pro_num_);
    NewWritableFile(profile, &queue_);

    {
      WriteLock(&version_->rwlock_);
      version_->pro_offset_ = 0;
      version_->pro_num_ = pro_num_;
      version_->StableSave();
      // version_->debug();
    }
  }

  int pro_offset;
  s = Produce(Slice(item.data(), item.size()), &pro_offset);
  if (s.ok()) {
    WriteLock(&version_->rwlock_);
    version_->pro_offset_ = pro_offset;
    version_->StableSave();
  }

  return s;
}

Status BinlogImpl::EmitPhysicalRecord(RecordType t, const char* ptr, size_t n, int* temp_pro_offset) {
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

Status BinlogImpl::Produce(const Slice& item, int* temp_pro_offset) {
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
        queue_->Append(Slice("\x00\x00\x00\x00\x00\x00\x00", leftover));
        *temp_pro_offset += leftover;
        // version_->StableSave();
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

Status BinlogImpl::AppendBlank(WritableFile* file, uint64_t len) {
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

Status BinlogImpl::SetProducerStatus(uint32_t pro_num, uint64_t pro_offset) {
  MutexLock l(&mutex_);

  // offset smaller than the first header
  if (pro_offset < kHeaderSize) {
    pro_offset = 0;
  }

  delete queue_;

  std::string init_profile = NewFileName(path_ + kBinlogPrefix, 0);
  if (FileExists(init_profile)) {
    DeleteFile(init_profile);
  }

  std::string profile = NewFileName(path_ + kBinlogPrefix, pro_num);
  if (FileExists(profile)) {
    DeleteFile(profile);
  }

  NewWritableFile(profile, &queue_);
  BinlogImpl::AppendBlank(queue_, pro_offset);

  pro_num_ = pro_num;

  {
    WriteLock(&version_->rwlock_);
    version_->pro_num_ = pro_num;
    version_->pro_offset_ = pro_offset;
    version_->StableSave();
  }

  InitOffset();
  return Status::OK();
}

BinlogReader* BinlogImpl::NewBinlogReader(uint32_t filenum, uint64_t offset) {
  // Check sync point
  uint32_t cur_filenum = 0;
  uint64_t cur_offset = 0;
  GetProducerStatus(&cur_filenum, &cur_offset);
  if (cur_filenum < filenum || (cur_filenum == filenum && cur_offset < offset)) {
    return NULL;
  }

  std::string confile = NewFileName(path_ + kBinlogPrefix, filenum);
  if (!pstd::FileExists(confile)) {
    // Not found binlog specified by filenum
    return NULL;
  }

  BinlogReaderImpl* reader = new BinlogReaderImpl(this, path_, filenum, offset);
  Status s = reader->Trim();
  if (!s.ok()) {
    log_info("Trim offset failed: %s", s.ToString().c_str());
    return NULL;
  }

  return reader;
}

BinlogReaderImpl::BinlogReaderImpl(Binlog* log, const std::string& path, uint32_t filenum, uint64_t offset)
    : log_(log),
      path_(path),
      filenum_(filenum),
      offset_(offset),
      should_exit_(false),
      initial_offset_(0),
      last_record_offset_(offset_ % kBlockSize),
      end_of_buffer_offset_(kBlockSize),
      queue_(NULL),
      backing_store_(new char[kBlockSize]) {
  std::string confile = NewFileName(path_ + kBinlogPrefix, filenum_);
  if (!NewSequentialFile(confile, &queue_).ok()) {
    log_info("Reader new sequtialfile failed");
  }
}

BinlogReaderImpl::~BinlogReaderImpl() {
  delete queue_;
  delete[] backing_store_;
}

Status BinlogReaderImpl::Trim() {
  Status s;
  uint64_t start_block = (offset_ / kBlockSize) * kBlockSize;
  s = queue_->Skip(start_block);
  if (!s.ok()) {
    return s;
  }
  uint64_t block_offset = offset_ % kBlockSize;
  uint64_t ret = 0;
  uint64_t res = 0;

  while (true) {
    if (res >= block_offset) {
      offset_ = start_block + res;
      break;
    }
    ret = GetNext(s);
    if (!s.ok()) {
      return s;
    }
    res += ret;
  }
  last_record_offset_ = offset_ % kBlockSize;

  return Status::OK();
}

uint64_t BinlogReaderImpl::GetNext(Status& result) {
  uint64_t offset = 0;
  Status s;

  while (true) {
    buffer_.clear();
    s = queue_->Read(kHeaderSize, &buffer_, backing_store_);
    if (!s.ok()) {
      result = s;
      return 0;
    }

    const char* header = buffer_.data();
    const uint32_t a = static_cast<uint32_t>(header[0]) & 0xff;
    const uint32_t b = static_cast<uint32_t>(header[1]) & 0xff;
    const uint32_t c = static_cast<uint32_t>(header[2]) & 0xff;
    const unsigned int type = header[7];
    const uint32_t length = a | (b << 8) | (c << 16);

    if (type == kFullType) {
      s = queue_->Read(length, &buffer_, backing_store_);
      offset += kHeaderSize + length;
      break;
    } else if (type == kFirstType) {
      s = queue_->Read(length, &buffer_, backing_store_);
      offset += kHeaderSize + length;
    } else if (type == kMiddleType) {
      s = queue_->Read(length, &buffer_, backing_store_);
      offset += kHeaderSize + length;
    } else if (type == kLastType) {
      s = queue_->Read(length, &buffer_, backing_store_);
      offset += kHeaderSize + length;
      break;
    } else {
      break;
    }
  }
  result = s;
  return offset;
}

unsigned int BinlogReaderImpl::ReadPhysicalRecord(Slice* result) {
  Status s;
  uint64_t zero_space = end_of_buffer_offset_ - last_record_offset_;
  if (zero_space <= kHeaderSize) {
    queue_->Skip(zero_space);
    offset_ += zero_space;
    last_record_offset_ = 0;
  }
  buffer_.clear();
  s = queue_->Read(kHeaderSize, &buffer_, backing_store_);
  if (s.IsEndFile()) {
    return kEof;
  } else if (!s.ok()) {
    return kBadRecord;
  }

  const char* header = buffer_.data();
  const uint32_t a = static_cast<uint32_t>(header[0]) & 0xff;
  const uint32_t b = static_cast<uint32_t>(header[1]) & 0xff;
  const uint32_t c = static_cast<uint32_t>(header[2]) & 0xff;
  const unsigned int type = header[7];
  const uint32_t length = a | (b << 8) | (c << 16);
  if (type == kZeroType || length == 0) {
    buffer_.clear();
    return kOldRecord;
  }

  buffer_.clear();
  // std::cout<<"2 --> offset_: "<<offset_<<" last_record_offset_: "<<last_record_offset_<<std::endl;
  s = queue_->Read(length, &buffer_, backing_store_);
  *result = Slice(buffer_.data(), buffer_.size());
  last_record_offset_ += kHeaderSize + length;
  if (s.ok()) {
    offset_ += (kHeaderSize + length);
  }
  return type;
}

Status BinlogReaderImpl::Consume(std::string& scratch) {
  Status s;
  if (last_record_offset_ < initial_offset_) {
    return Status::IOError("last_record_offset exceed");
  }

  Slice fragment;
  while (true) {
    const unsigned int record_type = ReadPhysicalRecord(&fragment);

    switch (record_type) {
      case kFullType:
        scratch = std::string(fragment.data(), fragment.size());
        s = Status::OK();
        break;
      case kFirstType:
        scratch.assign(fragment.data(), fragment.size());
        s = Status::NotFound("Middle Status");
        break;
      case kMiddleType:
        scratch.append(fragment.data(), fragment.size());
        s = Status::NotFound("Middle Status");
        break;
      case kLastType:
        scratch.append(fragment.data(), fragment.size());
        s = Status::OK();
        break;
      case kEof:
        return Status::EndFile("Eof");
      case kBadRecord:
        return Status::IOError("Data Corruption");
      case kOldRecord:
        return Status::EndFile("Eof");
      default:
        return Status::IOError("Unknow reason");
    }
    // TODO:do handler here
    if (s.ok()) {
      break;
    }
  }
  // DLOG(INFO) << "Binlog Sender consumer a msg: " << scratch;
  return Status::OK();
}

// Get a whole message;
// the status will be OK, IOError or Corruption;
Status BinlogReaderImpl::ReadRecord(std::string& scratch) {
  scratch.clear();
  Status s;
  uint32_t pro_num;
  uint64_t pro_offset;

  while (!should_exit_) {
    log_->GetProducerStatus(&pro_num, &pro_offset);
    if (filenum_ == pro_num && offset_ == pro_offset) {
      usleep(10000);
      continue;
    }

    s = Consume(scratch);
    if (s.IsEndFile()) {
      std::string confile = NewFileName(path_ + kBinlogPrefix, filenum_ + 1);

      // Roll to next File
      if (FileExists(confile)) {
        delete queue_;
        queue_ = NULL;
        NewSequentialFile(confile, &(queue_));

        filenum_++;
        offset_ = 0;
        initial_offset_ = 0;
        end_of_buffer_offset_ = kBlockSize;
        last_record_offset_ = offset_ % kBlockSize;
      } else {
        usleep(10000);
      }
    } else {
      break;
    }
  }

  if (should_exit_) {
    return Status::Corruption("should exit");
  }
  return s;
}

}  // namespace pstd
