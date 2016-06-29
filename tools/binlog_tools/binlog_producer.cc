
#include "binlog_producer.h"

#include <glog/logging.h>
#include <poll.h>
#include <sys/time.h>

#include "pika_define.h"

using slash::Status;
using slash::Slice;


BinlogProducer::BinlogProducer(const std::string& binlog_path)
  : pro_num_(0) ,
    queue_(NULL) {
    header_size_ = 1 + 3;
    Status s;
    slash::CreateDir(binlog_path);
    filename_ = binlog_path + kBinlogPrefix;
    const std::string manifest = binlog_path + kManifest;
    s = slash::NewRWFile(manifest, &versionfile_);
    version_ = new Version(versionfile_);

}

BinlogProducer::~BinlogProducer() {
  if(queue_) {
    delete queue_;
  }
}

Status BinlogProducer::LoadFile(uint32_t file) {
  pro_num_ = file;
  Status s;
  std::string profile = NewFileName(filename_, pro_num_);
  s = slash::NewWritableFile(profile, &queue_);
  if (s.ok()) {
    DLOG(INFO) << "new binlog:" << profile << " created";
    uint64_t filesize = queue_->Filesize();
    block_offset_ = filesize % kBlockSize;
  } else {
    DLOG(INFO) << "error when creating new binlog file:" << profile;
  }
  version_->pro_num_ = pro_num_;
  version_->pro_offset_ = 0;
  version_->StableSave();
  return Status::OK();
}

Status BinlogProducer::LoadNextFile() {
  if(queue_) {
    delete queue_;
    queue_ = NULL;
  }
  pro_num_ ++;
  Status s;
  std::string profile = NewFileName(filename_, pro_num_);
  s = slash::NewWritableFile(profile, &queue_);
  if (s.ok()) {
    DLOG(INFO) << "new binlog:" << profile << " created";
    uint64_t filesize = queue_->Filesize();
    block_offset_ = filesize % kBlockSize;
  } else {
    DLOG(INFO) << "error when creating new binlog file:" << profile;
  }
  version_->pro_num_ = pro_num_;
  version_->pro_offset_ = 0;
  version_->StableSave();
  return Status::OK();
}




Status BinlogProducer::Produce(const Slice &item, int *temp_pro_offset) {
  Status s;
  const char *ptr = item.data();
  size_t left = item.size();
  bool begin = true;

  *temp_pro_offset = version_->pro_offset_;
  do {
    const int leftover = static_cast<int>(kBlockSize) - block_offset_;
    assert(leftover >= 0);
    if (static_cast<size_t>(leftover) < header_size_) {
      if (leftover > 0) {
        queue_->Append(Slice("\x00\x00\x00\x00\x00\x00\x00", leftover));
        //version_->rise_pro_offset(leftover);
        *temp_pro_offset += leftover;
        //version_->StableSave();
      }
      block_offset_ = 0;
    }

    const size_t avail = kBlockSize - block_offset_ - header_size_;
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




Status BinlogProducer::Put(const std::string& item) {
  Status s;
  /* Check to roll log file */
  if (queue_ == NULL) {
      return Status::Corruption("no file is loaded yet");
  }
  int pro_offset;
  s = Produce(Slice(item.data(), item.size()), &pro_offset);
  if (s.ok()) {
    slash::RWLock(&(version_->rwlock_), true);
    //version_->plus_item_num();
    version_->pro_offset_ = pro_offset;
    //version_->set_pro_offset(pro_offset);
    version_->StableSave();
  }

  return s;
}


OldBinlogProducer::OldBinlogProducer(const std::string& binlog_path)
  : BinlogProducer(binlog_path) {
    header_size_ = 1 + 3;
}

OldBinlogProducer::~OldBinlogProducer() {
}

Status OldBinlogProducer::EmitPhysicalRecord(RecordType t, const char *ptr, size_t n, int *temp_pro_offset) {
    Status s;
    assert(n <= 0xffffff);

    char buf[header_size_];

    buf[0] = static_cast<char>(n & 0xff);
    buf[1] = static_cast<char>((n & 0xff00) >> 8);
    buf[2] = static_cast<char>(n >> 16);
    buf[3] = static_cast<char>(t);

    s = queue_->Append(Slice(buf, header_size_));
    if (s.ok()) {
        s = queue_->Append(Slice(ptr, n));
        if (s.ok()) {
            s = queue_->Flush();
        }
    }
    block_offset_ += static_cast<int>(header_size_ + n);
    // log_info("block_offset %d", (header_size_ + n));

    *temp_pro_offset += header_size_ + n;
    //version_->rise_pro_offset((uint64_t)(header_size_ + n));
    //version_->StableSave();
    return s;
}

NewBinlogProducer::NewBinlogProducer(const std::string& binlog_path)
  : BinlogProducer(binlog_path) {
    header_size_ = 1 + 3 + 4;
}

NewBinlogProducer::~NewBinlogProducer() {
}

Status NewBinlogProducer::EmitPhysicalRecord(RecordType t, const char *ptr, size_t n, int *temp_pro_offset) {
    Status s;
    assert(n <= 0xffffff);

    char buf[header_size_];

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

    s = queue_->Append(Slice(buf, header_size_));
    if (s.ok()) {
        s = queue_->Append(Slice(ptr, n));
        if (s.ok()) {
            s = queue_->Flush();
        }
    }
    block_offset_ += static_cast<int>(header_size_ + n);
    // log_info("block_offset %d", (header_size_ + n));

    *temp_pro_offset += header_size_ + n;
    //version_->rise_pro_offset((uint64_t)(header_size_ + n));
    //version_->StableSave();
    return s;
}

