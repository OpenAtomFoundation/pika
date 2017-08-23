// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_binlog_sender_thread.h"

#include <glog/logging.h>
#include <poll.h>

#include "include/pika_server.h"
#include "include/pika_define.h"
#include "include/pika_binlog_sender_thread.h"
#include "include/pika_master_conn.h"
#include "pink/include/redis_cli.h"

extern PikaServer* g_pika_server;

PikaBinlogSenderThread::PikaBinlogSenderThread(const std::string &ip, int port,
                                               slash::SequentialFile *queue,
                                               uint32_t filenum,
                                               uint64_t con_offset)
    : con_offset_(con_offset),
      filenum_(filenum),
      initial_offset_(0),
      end_of_buffer_offset_(kBlockSize),
      queue_(queue),
      backing_store_(new char[kBlockSize]),
      buffer_(),
      ip_(ip),
      port_(port),
      timeout_ms_(35000) {
  cli_ = pink::NewRedisCli();
  last_record_offset_ = con_offset % kBlockSize;
  set_thread_name("BinlogSender");
}

PikaBinlogSenderThread::~PikaBinlogSenderThread() {
  StopThread();
  delete cli_;
  delete[] backing_store_;
  delete queue_;
  LOG(INFO) << "a BinlogSender thread " << thread_id() << " exit!";
}

int PikaBinlogSenderThread::trim() {
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
    ret = get_next(is_error);
    if (is_error == true) {
      return -1;
    }
    res += ret;
  }
  last_record_offset_ = con_offset_ % kBlockSize;

  return 0;
}

uint64_t PikaBinlogSenderThread::get_next(bool &is_error) {
  uint64_t offset = 0;
  slash::Status s;
  is_error = false;

  while (true) {
    buffer_.clear();
    s = queue_->Read(kHeaderSize, &buffer_, backing_store_);
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
  return offset;
}

unsigned int PikaBinlogSenderThread::ReadPhysicalRecord(slash::Slice *result) {
  slash::Status s;
  if (end_of_buffer_offset_ - last_record_offset_ <= kHeaderSize) {
    queue_->Skip(end_of_buffer_offset_ - last_record_offset_);
    con_offset_ += (end_of_buffer_offset_ - last_record_offset_);
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
  //std::cout<<"2 --> con_offset_: "<<con_offset_<<" last_record_offset_: "<<last_record_offset_<<std::endl;
  s = queue_->Read(length, &buffer_, backing_store_);
  *result = slash::Slice(buffer_.data(), buffer_.size());
  last_record_offset_ += kHeaderSize + length;
  if (s.ok()) {
    con_offset_ += (kHeaderSize + length);
  }
  return type;
}

Status PikaBinlogSenderThread::Consume(std::string &scratch) {
  Status s;
  if (last_record_offset_ < initial_offset_) {
    return slash::Status::IOError("last_record_offset exceed");
  }

  slash::Slice fragment;
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
  //DLOG(INFO) << "Binlog Sender consumer a msg: " << scratch;
  return Status::OK();
}

// Get a whole message; 
// the status will be OK, IOError or Corruption;
Status PikaBinlogSenderThread::Parse(std::string &scratch) {
  scratch.clear();
  Status s;
  uint32_t pro_num;
  uint64_t pro_offset;

  Binlog* logger = g_pika_server->logger_;
  while (!should_stop()) {
    logger->GetProducerStatus(&pro_num, &pro_offset);
    if (filenum_ == pro_num && con_offset_ == pro_offset) {
      //DLOG(INFO) << "BinlogSender Parse no new msg, filenum_" << filenum_ << ", con_offset " << con_offset_;
      usleep(10000);
      continue;
    }

    //DLOG(INFO) << "BinlogSender start Parse a msg               filenum_" << filenum_ << ", con_offset " << con_offset_;
    s = Consume(scratch);

    //DLOG(INFO) << "BinlogSender after Parse a msg return " << s.ToString() << " filenum_" << filenum_ << ", con_offset " << con_offset_;
    if (s.IsEndFile()) {
      std::string confile = NewFileName(g_pika_server->logger_->filename, filenum_ + 1);

      // Roll to next File
      if (slash::FileExists(confile)) {
        DLOG(INFO) << "BinlogSender roll to new binlog" << confile;
        delete queue_;
        queue_ = NULL;

        slash::NewSequentialFile(confile, &(queue_));

        filenum_++;
        con_offset_ = 0;
        initial_offset_ = 0;
        end_of_buffer_offset_ = kBlockSize;
        last_record_offset_ = con_offset_ % kBlockSize;
      } else {
        usleep(10000);
      }
    } else {
      break;
    }
  }
    
  if (should_stop()) {
    return Status::Corruption("should exit");
  }
  return s;
}

// When we encount
void* PikaBinlogSenderThread::ThreadMain() {
  Status s, result;
  bool last_send_flag = true;
  std::string scratch;
  scratch.reserve(1024 * 1024);

  while (!should_stop()) {
    sleep(1);
    // 1. Connect to slave
    result = cli_->Connect(ip_, port_, g_pika_server->host());
    LOG(INFO) << "BinlogSender Connect slave(" << ip_ << ":" << port_ << ") " << result.ToString();

    if (result.ok()) {
      cli_->set_send_timeout(timeout_ms_);
      while (true) {
        // 2. Should Parse new msg;
        if (last_send_flag) {
          s = Parse(scratch);
          //DLOG(INFO) << "BinlogSender Parse, return " << s.ToString();

          if (s.IsCorruption()) {     // should exit
            LOG(WARNING) << "BinlogSender Parse failed, will exit, error: " << s.ToString();
            //close(sockfd_);
            break;
          } else if (s.IsIOError()) {
            LOG(WARNING) << "BinlogSender Parse error, " << s.ToString();
            continue;
          }
        }

        // Parse binlog
        std::vector<std::string> items;
        std::string token, delimiter = "\r\n", scratch_copy = scratch;
        size_t pos = 0;
        while ((pos = scratch_copy.find(delimiter)) != std::string::npos) {
          token = scratch_copy.substr(0, pos);
          items.push_back(token);
          scratch_copy.erase(0, pos + delimiter.length());
        }
        items.push_back(scratch_copy);
        std::string binlog_sid = items[items.size() - 6];
        LOG(INFO) << "This binlog server id: " << binlog_sid;

        // If this binlog from the peer-master, can not resend to the peer-master
        if (std::atoi(binlog_sid.c_str()) == g_pika_server->DoubleMasterSid()) {
          LOG(INFO) << "This binlog from the peer-master";
          continue;
        }

        // 3. After successful parse, we send msg;
        result = cli_->Send(&scratch);
        if (result.ok()) {
          last_send_flag = true;
          LOG(INFO) << "BinlogSender send slave(" << ip_ << ":" << port_ << ") " << scratch << " " << result.ToString();
        } else {
          last_send_flag = false;
          DLOG(INFO) << "BinlogSender send slave(" << ip_ << ":" << port_ << ") failed,  " << result.ToString();
          break;
        }
      }
    }

    // error
    cli_->Close();
    sleep(1);
  }
  return NULL;
}

