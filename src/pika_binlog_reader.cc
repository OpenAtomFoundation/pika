// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_binlog_reader.h"

#include <glog/logging.h>

PikaBinlogReader::PikaBinlogReader(uint32_t cur_filenum, uint64_t cur_offset)
    : cur_filenum_(cur_filenum),
      cur_offset_(cur_offset),
      logger_(nullptr),
      queue_(nullptr),
      backing_store_(new char[kBlockSize]),
      buffer_() {
  last_record_offset_ = cur_offset % kBlockSize;
  pthread_rwlock_init(&rwlock_, nullptr);
}

PikaBinlogReader::PikaBinlogReader()
    : cur_filenum_(0),
      cur_offset_(0),
      logger_(nullptr),
      queue_(nullptr),
      backing_store_(new char[kBlockSize]),
      buffer_() {
  last_record_offset_ = 0 % kBlockSize;
  pthread_rwlock_init(&rwlock_, nullptr);
}

PikaBinlogReader::~PikaBinlogReader() {
  delete[] backing_store_;
  delete queue_;
  pthread_rwlock_destroy(&rwlock_);
}

void PikaBinlogReader::GetReaderStatus(uint32_t* cur_filenum, uint64_t* cur_offset) {
  pstd::RWLock l(&(rwlock_), false);
  *cur_filenum = cur_filenum_;
  *cur_offset = cur_offset_;
}

bool PikaBinlogReader::ReadToTheEnd() {
  uint32_t pro_num;
  uint64_t pro_offset;
  logger_->GetProducerStatus(&pro_num, &pro_offset);
  pstd::RWLock l(&(rwlock_), false);
  return (pro_num == cur_filenum_ && pro_offset == cur_offset_);
}

int PikaBinlogReader::Seek(std::shared_ptr<Binlog> logger, uint32_t filenum, uint64_t offset) {
  std::string confile = NewFileName(logger->filename(), filenum);
  if (!pstd::FileExists(confile)) {
    LOG(WARNING) << confile << " not exits";
    return -1;
  }
  pstd::SequentialFile* readfile;
  if (!pstd::NewSequentialFile(confile, &readfile).ok()) {
    LOG(WARNING) << "New swquential " << confile << " failed";
    return -1;
  }
  if (queue_) {
    delete queue_;
  }
  queue_ = readfile;
  logger_ = logger;

  pstd::RWLock l(&(rwlock_), true);
  cur_filenum_ = filenum;
  cur_offset_ = offset;
  last_record_offset_ = cur_filenum_ % kBlockSize;

  pstd::Status s;
  uint64_t start_block = (cur_offset_ / kBlockSize) * kBlockSize;
  s = queue_->Skip((cur_offset_ / kBlockSize) * kBlockSize);
  uint64_t block_offset = cur_offset_ % kBlockSize;
  uint64_t ret = 0;
  uint64_t res = 0;
  bool is_error = false;

  while (true) {
    if (res >= block_offset) {
      cur_offset_ = start_block + res;
      break;
    }
    ret = 0;
    is_error = GetNext(&ret);
    if (is_error == true) {
      return -1;
    }
    res += ret;
  }
  last_record_offset_ = cur_offset_ % kBlockSize;
  return 0;
}

bool PikaBinlogReader::GetNext(uint64_t* size) {
  uint64_t offset = 0;
  pstd::Status s;
  bool is_error = false;

  while (true) {
    buffer_.clear();
    s = queue_->Read(kHeaderSize, &buffer_, backing_store_);
    if (!s.ok()) {
      is_error = true;
      return is_error;
    }

    const char* header = buffer_.data();
    const uint32_t a = static_cast<uint32_t>(header[0]) & 0xff;
    const uint32_t b = static_cast<uint32_t>(header[1]) & 0xff;
    const uint32_t c = static_cast<uint32_t>(header[2]) & 0xff;
    const unsigned int type = header[7];
    const uint32_t length = a | (b << 8) | (c << 16);

    if (length > (kBlockSize - kHeaderSize)) return true;

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
    } else if (type == kBadRecord) {
      s = queue_->Read(length, &buffer_, backing_store_);
      offset += kHeaderSize + length;
      break;
    } else {
      is_error = true;
      break;
    }
  }
  *size = offset;
  return is_error;
}

unsigned int PikaBinlogReader::ReadPhysicalRecord(pstd::Slice* result, uint32_t* filenum, uint64_t* offset) {
  pstd::Status s;
  if (kBlockSize - last_record_offset_ <= kHeaderSize) {
    queue_->Skip(kBlockSize - last_record_offset_);
    pstd::RWLock l(&(rwlock_), true);
    cur_offset_ += (kBlockSize - last_record_offset_);
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

  if (length > (kBlockSize - kHeaderSize)) return kBadRecord;

  if (type == kZeroType || length == 0) {
    buffer_.clear();
    return kOldRecord;
  }

  buffer_.clear();
  s = queue_->Read(length, &buffer_, backing_store_);
  *result = pstd::Slice(buffer_.data(), buffer_.size());
  last_record_offset_ += kHeaderSize + length;
  if (s.ok()) {
    pstd::RWLock l(&(rwlock_), true);
    *filenum = cur_filenum_;
    cur_offset_ += (kHeaderSize + length);
    *offset = cur_offset_;
  }
  return type;
}

Status PikaBinlogReader::Consume(std::string* scratch, uint32_t* filenum, uint64_t* offset) {
  Status s;

  pstd::Slice fragment;
  while (true) {
    const unsigned int record_type = ReadPhysicalRecord(&fragment, filenum, offset);

    switch (record_type) {
      case kFullType:
        *scratch = std::string(fragment.data(), fragment.size());
        s = Status::OK();
        break;
      case kFirstType:
        scratch->assign(fragment.data(), fragment.size());
        s = Status::NotFound("Middle Status");
        break;
      case kMiddleType:
        scratch->append(fragment.data(), fragment.size());
        s = Status::NotFound("Middle Status");
        break;
      case kLastType:
        scratch->append(fragment.data(), fragment.size());
        s = Status::OK();
        break;
      case kEof:
        return Status::EndFile("Eof");
      case kBadRecord:
        LOG(WARNING)
            << "Read BadRecord record, will decode failed, this record may dbsync padded record, not processed here";
        return Status::IOError("Data Corruption");
      case kOldRecord:
        return Status::EndFile("Eof");
      default:
        return Status::IOError("Unknow reason");
    }
    if (s.ok()) {
      break;
    }
  }
  // DLOG(INFO) << "Binlog Sender consumer a msg: " << scratch;
  return Status::OK();
}

// Get a whole message;
// Append to scratch;
// the status will be OK, IOError or Corruption, EndFile;
Status PikaBinlogReader::Get(std::string* scratch, uint32_t* filenum, uint64_t* offset) {
  if (logger_ == nullptr || queue_ == nullptr) {
    return Status::Corruption("Not seek");
  }
  scratch->clear();
  Status s = Status::OK();

  do {
    if (ReadToTheEnd()) {
      return Status::EndFile("End of cur log file");
    }
    s = Consume(scratch, filenum, offset);
    if (s.IsEndFile()) {
      std::string confile = NewFileName(logger_->filename(), cur_filenum_ + 1);

      // sleep 10ms wait produce thread generate the new binlog
      usleep(10000);

      // Roll to next file need retry;
      if (pstd::FileExists(confile)) {
        DLOG(INFO) << "BinlogSender roll to new binlog" << confile;
        delete queue_;
        queue_ = nullptr;

        pstd::NewSequentialFile(confile, &(queue_));
        {
          pstd::RWLock l(&(rwlock_), true);
          cur_filenum_++;
          cur_offset_ = 0;
        }
        last_record_offset_ = 0;
      } else {
        return Status::IOError("File Does Not Exists");
      }
    } else {
      break;
    }
  } while (s.IsEndFile());

  return Status::OK();
}
