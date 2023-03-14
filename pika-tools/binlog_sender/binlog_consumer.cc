//  Copyright (c) 2018-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "binlog_consumer.h"

BinlogConsumer::BinlogConsumer(const std::string& binlog_path,
                               uint32_t first_filenum,
                               uint32_t last_filenum,
                               uint64_t offset)
    : filename_(binlog_path + kBinlogPrefix),
      first_filenum_(first_filenum),
      last_filenum_(last_filenum),
      current_offset_(offset),
      backing_store_(new char[kBlockSize]),
      queue_(nullptr) {
};

BinlogConsumer::~BinlogConsumer() {
  delete[] backing_store_;
  delete queue_;
}

std::string BinlogConsumer::NewFileName(const std::string& name,
                                        const uint32_t current) {
  char buf[256];
  snprintf(buf, sizeof(buf), "%s%u", name.c_str(), current);
  return std::string(buf);
}

bool BinlogConsumer::Init() {
  std::string profile;
  for (size_t idx = first_filenum_; idx <= last_filenum_; ++idx) {
    profile = NewFileName(filename_, idx);
    if (!slash::FileExists(profile)) {
      fprintf(stderr, "Binlog %s not exists\n", profile.c_str());
      return false;
    }
  }

  current_filenum_ = first_filenum_;
  profile = NewFileName(filename_, current_filenum_);
  slash::Status s = slash::NewSequentialFile(profile, &queue_);
  if (!s.ok()) {
    return false;
  } else {
    return true;
  }
}

bool BinlogConsumer::trim() {
  slash::Status s;
  uint64_t start_block = (current_offset_ / kBlockSize) * kBlockSize;
  s = queue_->Skip((current_offset_ / kBlockSize) * kBlockSize);
  if (!s.ok()) {
    return false;
  }

  uint64_t block_offset = current_offset_ % kBlockSize;
  uint64_t offset = 0;
  uint64_t res = 0;
  bool is_error = false;

  while (true) {
    if (res >= block_offset) {
      current_offset_ = start_block + res;
      break;
    }
    offset = get_next(&is_error);
    if (is_error == true) {
      return false;
    }
    res += offset;
  }
  last_record_offset_ = current_offset_ % kBlockSize;
  return true;
}

uint32_t BinlogConsumer::current_filenum() {
  return current_filenum_;
}

uint64_t BinlogConsumer::current_offset() {
  return current_offset_;
}

uint64_t BinlogConsumer::get_next(bool* is_error) {
  uint64_t offset = 0;
  slash::Status s;
  *is_error = false;

  while (true) {
    buffer_.clear();
    s = queue_->Read(kHeaderSize, &buffer_, backing_store_);
    if (!s.ok()) {
      *is_error = true;
      break;
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
      *is_error = true;
      break;
    }
  }
  return offset;
}

uint32_t BinlogConsumer::ReadPhysicalRecord(slash::Slice *result) {
  slash::Status s;
  if (kBlockSize - last_record_offset_ <= kHeaderSize) {
    queue_->Skip(kBlockSize - last_record_offset_);
    current_offset_ += (kBlockSize - last_record_offset_);
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
    current_offset_ += (kHeaderSize + length);
  }
  return type;
}

slash::Status BinlogConsumer::Consume(std::string* scratch) {
  slash::Status s;

  slash::Slice fragment;
  while (true) {
    const uint32_t record_type = ReadPhysicalRecord(&fragment);

    switch (record_type) {
      case kFullType:
        *scratch = std::string(fragment.data(), fragment.size());
        s = slash::Status::OK();
        break;
      case kFirstType:
        scratch->assign(fragment.data(), fragment.size());
        s = slash::Status::NotFound("Middle Status");
        break;
      case kMiddleType:
        scratch->append(fragment.data(), fragment.size());
        s = slash::Status::NotFound("Middle Status");
        break;
      case kLastType:
        scratch->append(fragment.data(), fragment.size());
        s = slash::Status::OK();
        break;
      case kEof:
        return slash::Status::EndFile("Eof");
      case kBadRecord:
        return slash::Status::IOError("Data Corruption");
      case kOldRecord:
        return slash::Status::EndFile("Eof");
      default:
        return slash::Status::IOError("Unknow reason");
    }
    if (s.ok()) {
      break;
    }
  }
  return slash::Status::OK();
}


// Get a whole message; 
// the status will be OK, IOError or Corruption;
slash::Status BinlogConsumer::Parse(std::string* scratch) {
  slash::Status s;
  scratch->clear();
  while (true) {

    s = Consume(scratch);

    if (s.IsEndFile()) {

      if (current_filenum_ == last_filenum_) {
        return slash::Status::Complete("finish");
      } else {
        std::string confile = NewFileName(filename_, current_filenum_ + 1);

        // Roll to next File
        if (slash::FileExists(confile)) {
          //DLOG(INFO) << "BinlogSender roll to new binlog" << confile;
          delete queue_;
          queue_ = NULL;

          slash::NewSequentialFile(confile, &queue_);

          current_filenum_++;
          current_offset_ = 0;
          last_record_offset_ = 0;
        } else {
          return slash::Status::NotFound("not found");
        }
      }
    } else {
      break;
    }
  }
  return s;
}


      
