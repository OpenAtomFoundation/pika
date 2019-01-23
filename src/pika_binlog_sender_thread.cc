// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_binlog_sender_thread.h"

#include "pink/include/redis_cli.h"
#include "slash/include/slash_coding.h"

#include "include/pika_server.h"

extern PikaServer* g_pika_server;

PikaBinlogSenderThread::PikaBinlogSenderThread(const std::string &ip, int port,
                                               int64_t sid,
                                               slash::SequentialFile *queue,
                                               uint32_t filenum,
                                               uint64_t con_offset)
    : filenum_(filenum),
      con_offset_(con_offset),
      queue_(queue),
      backing_store_(new char[kBlockSize]),
      buffer_(),
      ip_(ip),
      port_(port),
      sid_(sid),
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
  if (kBlockSize - last_record_offset_ <= kHeaderSize) {
    queue_->Skip(kBlockSize - last_record_offset_);
    con_offset_ += (kBlockSize - last_record_offset_);
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
        last_record_offset_ = 0;
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
  std::string header, scratch, transfer;
  scratch.reserve(1024 * 1024);

  while (!should_stop()) {
    sleep(2);
    // 1. Connect to slave
    result = cli_->Connect(ip_, port_, "");
    LOG(INFO) << "BinlogSender Connect slave(" << ip_ << ":" << port_ << ") " << result.ToString();

    // 2. Auth
    if (result.ok()) {
      cli_->set_send_timeout(timeout_ms_);
      // Auth sid
      std::string auth_cmd;
      pink::RedisCmdArgsType argv;
      argv.push_back("auth");
      argv.push_back(std::to_string(sid_));
      pink::SerializeRedisCommand(argv, &auth_cmd);
      header.clear();
      slash::PutFixed16(&header, TransferOperate::kTypeAuth);
      slash::PutFixed32(&header, auth_cmd.size());
      transfer = header + auth_cmd;
      result = cli_->Send(&transfer);
      if (!result.ok()) {
        LOG(WARNING) << "BinlogSender send slave(" << ip_ << ":" << port_ << ") failed,  " << result.ToString();
        break;
      }
      while (true) {
        // 3. Should Parse new msg;
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
        BinlogItem binlog_item;
        PikaBinlogTransverter::BinlogDecode(BinlogType::TypeFirst,
                                            scratch,
                                            &binlog_item);

        // 4. After successful parse, we send msg;
        header.clear();
        slash::PutFixed16(&header, TransferOperate::kTypeBinlog);
        slash::PutFixed32(&header, scratch.size());
        transfer = header + scratch;
        result = cli_->Send(&transfer);
        if (result.ok()) {
          last_send_flag = true;
        } else {
          last_send_flag = false;
          LOG(WARNING) << "BinlogSender send slave(" << ip_ << ":" << port_ << ") failed,  " << result.ToString();
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
