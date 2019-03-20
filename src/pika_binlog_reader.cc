// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_binlog_reader.h"

#include <glog/logging.h>

PikaBinlogReader::PikaBinlogReader(slash::SequentialFile *queue,
    std::shared_ptr<Binlog> logger,
    uint32_t cur_filenum,
    uint64_t cur_offset)
    : cur_filenum_(cur_filenum),
      cur_offset_(cur_offset),
      logger_(logger),
      queue_(queue),
      backing_store_(new char[kBlockSize]),
      buffer_() {
  last_record_offset_ = cur_offset % kBlockSize;
  pthread_rwlock_init(&rwlock_, NULL);
}

PikaBinlogReader::~PikaBinlogReader() {
  delete[] backing_store_;
  delete queue_;
  pthread_rwlock_destroy(&rwlock_);
}

void PikaBinlogReader::GetReaderStatus(uint32_t* cur_filenum, uint64_t* cur_offset) {
  slash::RWLock(&(rwlock_), false);
  *cur_filenum = cur_filenum_;
  *cur_offset = cur_offset_;
}

bool PikaBinlogReader::ReadToTheEnd() {
  uint32_t pro_num;
  uint64_t pro_offset;
  logger_->GetProducerStatus(&pro_num, &pro_offset);
  slash::RWLock(&(rwlock_), false);
  return (pro_num == cur_filenum_ && pro_offset == cur_offset_);
}

int PikaBinlogReader::Seek() {
  slash::RWLock(&(rwlock_), true);
  slash::Status s;
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
  slash::Status s;
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
      is_error = true;
      break;
    }
  }
  *size = offset;
  return is_error;
}

unsigned int PikaBinlogReader::ReadPhysicalRecord(slash::Slice *result, uint32_t* filenum, uint64_t* offset) {
  slash::Status s;
  if (kBlockSize - last_record_offset_ <= kHeaderSize) {
    queue_->Skip(kBlockSize - last_record_offset_);
    slash::RWLock(&(rwlock_), true);
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
  if (type == kZeroType || length == 0) {
    buffer_.clear();
    return kOldRecord;
  }

  buffer_.clear();
  s = queue_->Read(length, &buffer_, backing_store_);
  *result = slash::Slice(buffer_.data(), buffer_.size());
  last_record_offset_ += kHeaderSize + length;
  if (s.ok()) {
    slash::RWLock(&(rwlock_), true);
    *filenum = cur_filenum_;
    cur_offset_ += (kHeaderSize + length);
    *offset = cur_offset_;
  }
  return type;
}

Status PikaBinlogReader::Consume(std::string* scratch, uint32_t* filenum, uint64_t* offset) {
  Status s;

  slash::Slice fragment;
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
  scratch->clear();
  Status s = Status::OK();
  uint32_t pro_num;
  uint64_t pro_offset;

  logger_->GetProducerStatus(&pro_num, &pro_offset);
  {
  slash::RWLock(&(rwlock_), false);
  if (cur_filenum_ == pro_num && cur_offset_ == pro_offset) {
    return Status::EndFile("End of cur log file");
  }
  }
  int retry = 0;
  while (retry == 0 || ((retry == 1) && s.IsEndFile())) {
    retry++;
    s = Consume(scratch, filenum, offset);

    if (s.IsEndFile()) {
      std::string confile;
      {
      slash::RWLock(&(rwlock_), false);
      if (cur_filenum_ == pro_num) {
        return Status::EndFile("End of cur log file");
      }
      confile = NewFileName(logger_->filename, cur_filenum_ + 1);
      }

      // Roll to next File need retry
      if (slash::FileExists(confile)) {
        DLOG(INFO) << "BinlogSender roll to new binlog" << confile;
        delete queue_;
        queue_ = NULL;

        slash::NewSequentialFile(confile, &(queue_));
        {
        slash::RWLock(&(rwlock_), true);
        cur_filenum_++;
        cur_offset_ = 0;
        }
        last_record_offset_ = 0;
      } else {
        return Status::IOError("File Does Not Exists");
      }
    } else if (!s.ok()) {
      return Status::Corruption("Data Corruption");
    }
  }
  return Status::OK();
}


