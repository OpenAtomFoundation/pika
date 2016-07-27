
#include "binlog_consumer.h"

#include <glog/logging.h>
#include <poll.h>
#include <iostream>

#include "pika_define.h"

using slash::Status;
using slash::Slice;


BinlogConsumer::BinlogConsumer(Binlog* logger)
  : logger_(logger),
    con_offset_(0),
    pro_num_(0),
    pro_offset_(0),
    initial_offset_(0),
    end_of_buffer_offset_(kBlockSize),
    queue_(NULL),
    backing_store_(new char[kBlockSize]),
    buffer_() {
      last_record_offset_ = con_offset_ % kBlockSize;
      pthread_rwlock_init(&rwlock_, NULL);
      logger_->GetProducerStatus(&pro_num_, &pro_offset_);

}

BinlogConsumer::~BinlogConsumer() {
  delete queue_;
  pthread_rwlock_destroy(&rwlock_);
  delete [] backing_store_;
}

Status BinlogConsumer::LoadFile(uint32_t file) {
  if (queue_ != NULL) {
    delete queue_;
  }
  current_file_ = file;
  slash::SequentialFile *readfile;
  std::string confile = NewFileName(logger_->filename, current_file_);
  if(!slash::FileExists(confile)){
    return Status::Corruption("error:binlog to consume does not exist");
  }
  if(slash::NewSequentialFile(confile, &readfile).ok()) {
    queue_ = readfile;
    DLOG(INFO) << "binlog file " << confile << " loaded to be parsed ";
    return Status::OK();
  } else {
    return Status::Corruption("error loading binlog file");
  }
}

int BinlogConsumer::Trim() {
  slash::Status s;
  uint64_t start_block = (con_offset_ / kBlockSize) * kBlockSize;
  s = queue_->Skip((con_offset_ / kBlockSize) * kBlockSize);
  uint64_t block_offset = con_offset_ % kBlockSize;
  uint64_t ret = 0;
  uint64_t res = 0;
  bool is_error = false;

  while (true) {
    if (res >= block_offset) {
      con_offset_ = start_block + res;
      break;
    }
    ret = GetNext(is_error);
    if (is_error == true) {
      return -1;
    }
    res += ret;
  }
  last_record_offset_ = con_offset_ % kBlockSize;

  return 0;
}



Status BinlogConsumer::Consume(std::string &scratch, uint64_t* produce_time) {
  Status s;
  if (last_record_offset_ < initial_offset_) {
    return slash::Status::IOError("last_record_offset exceed");
  }

  slash::Slice fragment;
  while (true) {
    const unsigned int record_type = ReadPhysicalRecord(&fragment, produce_time);

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
  return Status::OK();
}

// Get a whole message;
// the status will be OK, IOError or Corruption;
Status BinlogConsumer::Parse(std::string &scratch, uint64_t* produce_time) {
  scratch.clear();
  Status s;
  if (current_file_ == pro_num_ && con_offset_ == pro_offset_) {
    DLOG(INFO) << "BinlogConsumer Parse no new msg, filenum_" << current_file_ << ", con_offset " << con_offset_ << ", pro_num" << pro_num_ << ", pro_offset" << pro_offset_;
    s = Status::Complete("all binlog parsed");
    return s;
  }
  s = Consume(scratch, produce_time);
  return s;
}

Status BinlogConsumer::LoadNextFile(){
  std::string confile = NewFileName(logger_->filename, current_file_ + 1);
  if (slash::FileExists(confile)) {
    DLOG(INFO) << "BinlogConsumer roll to new binlog" << confile;
    delete queue_;
    queue_ = NULL;

    slash::NewSequentialFile(confile, &(queue_));

    current_file_ ++;
    con_offset_ = 0;
    initial_offset_ = 0;
    end_of_buffer_offset_ = kBlockSize;
    last_record_offset_ = con_offset_ % kBlockSize;
    return Status::OK();
  } else {
    DLOG(INFO) << "Can't find binlog file " << confile;
    return Status::Corruption("no binlog file exist to jump to");
  }
}

OldBinlogConsumer::OldBinlogConsumer(Binlog* logger)
  : BinlogConsumer(logger) {
    header_size_ = 1 + 3;
}

OldBinlogConsumer::~OldBinlogConsumer() {
}



uint64_t OldBinlogConsumer::GetNext(bool &is_error) {
  uint64_t offset = 0;
  slash::Status s;
  is_error = false;

  while (true) {
    buffer_.clear();
    s = queue_->Read(header_size_, &buffer_, backing_store_);
    if (!s.ok()) {
      is_error = true;
    }

    const char* header = buffer_.data();
    const uint32_t a = static_cast<uint32_t>(header[0]) & 0xff;
    const uint32_t b = static_cast<uint32_t>(header[1]) & 0xff;
    const uint32_t c = static_cast<uint32_t>(header[2]) & 0xff;
    const unsigned int type = header[3];
    const uint32_t length = a | (b << 8) | (c << 16);

    if (type == kFullType) {
      s = queue_->Read(length, &buffer_, backing_store_);
      offset += header_size_ + length;
      break;
    } else if (type == kFirstType) {
      s = queue_->Read(length, &buffer_, backing_store_);
      offset += header_size_ + length;
    } else if (type == kMiddleType) {
      s = queue_->Read(length, &buffer_, backing_store_);
      offset += header_size_ + length;
    } else if (type == kLastType) {
      s = queue_->Read(length, &buffer_, backing_store_);
      offset += header_size_ + length;
      break;
    } else {
      is_error = true;
      break;
    }
  }
  return offset;
}

unsigned int OldBinlogConsumer::ReadPhysicalRecord(slash::Slice *result,uint64_t* produce_time) {
  slash::Status s;
  if (end_of_buffer_offset_ - last_record_offset_ <= header_size_) {
    queue_->Skip(end_of_buffer_offset_ - last_record_offset_);
    con_offset_ += (end_of_buffer_offset_ - last_record_offset_);
    last_record_offset_ = 0;
  }
  buffer_.clear();
  s = queue_->Read(header_size_, &buffer_, backing_store_);
  if (s.IsEndFile()) {
    return kEof;
  } else if (!s.ok()) {
    return kBadRecord;
  }

  const char* header = buffer_.data();
  const uint32_t a = static_cast<uint32_t>(header[0]) & 0xff;
  const uint32_t b = static_cast<uint32_t>(header[1]) & 0xff;
  const uint32_t c = static_cast<uint32_t>(header[2]) & 0xff;
  const unsigned int type = header[3];
  const uint32_t length = a | (b << 8) | (c << 16);
  if (type == kZeroType || length == 0) {
    buffer_.clear();
    return kOldRecord;
  }

  buffer_.clear();
  //std::cout<<"2 --> con_offset_: "<<con_offset_<<" last_record_offset_: "<<last_record_offset_<<std::endl;
  s = queue_->Read(length, &buffer_, backing_store_);
  *result = slash::Slice(buffer_.data(), buffer_.size());
  last_record_offset_ += header_size_ + length;
  if (s.ok()) {
    con_offset_ += (header_size_ + length);
  }
  return type;
}

NewBinlogConsumer::NewBinlogConsumer(Binlog* logger)
  : BinlogConsumer(logger) {
    header_size_ = 1 + 3 + 4;
}

NewBinlogConsumer::~NewBinlogConsumer() {
}



uint64_t NewBinlogConsumer::GetNext(bool &is_error) {
  uint64_t offset = 0;
  slash::Status s;
  is_error = false;

  while (true) {
    buffer_.clear();
    s = queue_->Read(header_size_, &buffer_, backing_store_);
    if (!s.ok()) {
      is_error = true;
    }

    const char* header = buffer_.data();
    const uint32_t a = static_cast<uint32_t>(header[0]) & 0xff;
    const uint32_t b = static_cast<uint32_t>(header[1]) & 0xff;
    const uint32_t c = static_cast<uint32_t>(header[2]) & 0xff;
    const unsigned int type = header[7];
    const uint32_t length = a | (b << 8) | (c << 16);

    if (type == kFullType) {
      s = queue_->Read(length, &buffer_, backing_store_);
      offset += header_size_ + length;
      break;
    } else if (type == kFirstType) {
      s = queue_->Read(length, &buffer_, backing_store_);
      offset += header_size_ + length;
    } else if (type == kMiddleType) {
      s = queue_->Read(length, &buffer_, backing_store_);
      offset += header_size_ + length;
    } else if (type == kLastType) {
      s = queue_->Read(length, &buffer_, backing_store_);
      offset += header_size_ + length;
      break;
    } else {
      is_error = true;
      break;
    }
  }
  return offset;
}

unsigned int NewBinlogConsumer::ReadPhysicalRecord(slash::Slice *result, uint64_t* produce_time) {
  slash::Status s;
  if (end_of_buffer_offset_ - last_record_offset_ <= header_size_) {
    queue_->Skip(end_of_buffer_offset_ - last_record_offset_);
    con_offset_ += (end_of_buffer_offset_ - last_record_offset_);
    last_record_offset_ = 0;
  }
  buffer_.clear();
  s = queue_->Read(header_size_, &buffer_, backing_store_);
  if (s.IsEndFile()) {
    return kEof;
  } else if (!s.ok()) {
    return kBadRecord;
  }

  const char* header = buffer_.data();
  const uint32_t a = static_cast<uint32_t>(header[0]) & 0xff;
  const uint32_t b = static_cast<uint32_t>(header[1]) & 0xff;
  const uint32_t c = static_cast<uint32_t>(header[2]) & 0xff;
  const uint32_t d = static_cast<uint32_t>(header[3]) & 0xff;
  const uint32_t e = static_cast<uint32_t>(header[4]) & 0xff;
  const uint32_t f = static_cast<uint32_t>(header[5]) & 0xff;
  const uint32_t g = static_cast<uint32_t>(header[6]) & 0xff;
  const unsigned int type = header[7];
  const uint32_t length = a | (b << 8) | (c << 16);
  *produce_time = d | (e << 8) | (f << 16) | (g << 24);
  if (type == kZeroType || length == 0) {
    buffer_.clear();
    return kOldRecord;
  }

  buffer_.clear();
  s = queue_->Read(length, &buffer_, backing_store_);
  *result = slash::Slice(buffer_.data(), buffer_.size());
  last_record_offset_ += header_size_ + length;
  if (s.ok()) {
    con_offset_ += (header_size_ + length);
  }
  return type;
}

