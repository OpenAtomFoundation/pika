// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_hub_manager.h"

#include <glog/logging.h>
#include <poll.h>

#include "include/pika_server.h"
#include "include/pika_define.h"
#include "pink/include/redis_cli.h"

extern PikaServer* g_pika_server;

PikaHubManager::PikaHubManager(const std::set<std::string> &ips, int port,
                               int cron_interval)
    : hub_stage_(UNSTARTED),
      filenum_(0),
      con_offset_(0),
      hub_receiver_(new PikaHubReceiverThread(ips, port, cron_interval)){
  for (int i = 0; i < kMaxHubSender; i++) {
    sender_threads_[i].reset(new PikaHubSenderThread());
    sender_threads_[i]->SetNotifier(
        std::bind(&PikaHubManager::Notifier, this,
                  std::placeholders::_1, std::placeholders::_2,
                  std::placeholders::_3));
  }
}

Status PikaHubManager::AddHub(const std::string hub_ip, int hub_port,
                                uint32_t filenum, uint64_t con_offset) {
  std::string ip_port = slash::IpPortString(hub_ip, hub_port);
  LOG(INFO) << "Try add hub, " << ip_port;

  {
  slash::MutexLock l(&hub_mutex_);
  if (hub_stage_ == UNSTARTED) {
    hub_stage_ = STARTING;
    filenum_ = filenum;
    con_offset_ = con_offset;
    // need start
  } else if (hub_stage_ == STARTING) {
    // only handle one request
    return Status::OK();
  } else if (hub_stage_ == STARTED && 
             filenum != filenum_ && con_offset != con_offset_) {
    // need reset
    hub_stage_ = STARTING;
    filenum_ = filenum;
    con_offset_ = con_offset;
  } else {
    // already exist
    return Status::OK();
  }
  } // Unlock
  
  hub_ip_ = hub_ip;
  hub_port_ = hub_port;

  // start or reset senders
  Status s = ResetSenders();
  hub_stage_ = s.ok() ? STARTING : UNSTARTED;
  return s;
}

Status PikaHubManager::ResetSenders() {
  // Sanitize
  uint32_t cur_filenum = 0;
  uint64_t cur_offset = 0;
  g_pika_server->logger_->GetProducerStatus(&cur_filenum, &cur_offset);
  if (con_offset_ > g_pika_server->logger_->file_size() ||
      cur_filenum < filenum_ ||
      (cur_filenum == filenum_ && cur_offset < con_offset_)) {
    return Status::InvalidArgument("AddHubBinlogSender invalid binlog offset");
  }

  Status s;
  uint32_t f = filenum_, i = 0;
  for (; f <= cur_filenum && i <= kMaxHubSender;
       f++) {
    assert(hub_stage_ == STARTING);
    std::future<Status> fres = sender_threads_[i]->Reset(f, hub_ip_, hub_port_);
    s = fres.get();
    if (!s.ok()) {
      return s;
    }
  }

  sending_window_.first = filenum_;
  sending_window_.second = f - 1;
  DLOG(INFO) << "Hub sending window: " << sending_window_.first << "-" <<
    sending_window_.second;

  return s;
}

// Will be invoded in PikaHubSenderThread
void PikaHubManager::Notifier(PikaHubSenderThread* thread, uint32_t filenum,
                                bool* should_wait) {
  uint32_t cur_filenum;
  uint64_t cur_offset;
  g_pika_server->logger_->GetProducerStatus(&cur_filenum, &cur_offset);

  slash::MutexLock l(&hub_mutex_);
  if (filenum == sending_window_.first &&
      cur_filenum > sending_window_.second) {
    // FIXME(gaodq) Ignore result
    thread->Reset(sending_window_.second, hub_ip_, hub_port_);
    sending_window_.first++;
    sending_window_.second++;
    *should_wait = false;
  } else {
    *should_wait = true;
  }

  DLOG(INFO) << "Hub sending window: " << sending_window_.first << "-" <<
    sending_window_.second;
}

std::future<Status> PikaHubSenderThread::Reset(uint32_t filenum, const
                                               std::string& hub_ip, int hub_port) {
  reset_result_ = std::move(std::promise<Status>());

  reset_func_ = [&, filenum, hub_ip, hub_port]() {
    DLOG(INFO) << "Do reset, " << filenum << " " << hub_ip << ":" << hub_port;
    std::string confile = NewFileName(g_pika_server->logger_->filename, filenum);
    if (!slash::FileExists(confile)) {
      // Not found binlog specified by filenum
      reset_result_.set_value(Status::Incomplete("File does not exist"));
      return;
    }
    slash::SequentialFile* readfile = nullptr;
    if (!slash::NewSequentialFile(confile, &readfile).ok()) {
      reset_result_.set_value(Status::IOError("AddHubBinlogSender new sequtialfile"));
      return;
    }
    if (readfile != nullptr) {
      queue_.reset(readfile);
    } else {
      queue_.release();
    }

    hub_ip_ = hub_ip;
    hub_port_ = hub_port;
    con_offset_ = 0;
    filenum_ = filenum;
    last_record_offset_ = 0;

    if (trim() != 0) {
      reset_result_.set_value(Status::Corruption("Trim failed"));
      return;
    }

    reset_result_.set_value(Status::OK());
  };

  need_reset_ = true;

  // Try StartThread, maybe already started
  if (Thread::StartThread() != 0) {
    reset_result_.set_value(Status::Corruption("StartThread failed"));
  }
  DLOG(INFO) << "Start hub binlog sender thread";

  return reset_result_.get_future();
}

PikaHubSenderThread::PikaHubSenderThread()
    : need_reset_(false),
      backing_store_(new char[kBlockSize]),
      buffer_(),
      timeout_ms_(35000) {
  cli_.reset(pink::NewRedisCli());
  set_thread_name("BinlogSender");
}

PikaHubSenderThread::~PikaHubSenderThread() {
  StopThread();
  delete[] backing_store_;
  LOG(INFO) << "a BinlogSender thread " << thread_id() << " exit!";
}

int PikaHubSenderThread::trim() {
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

uint64_t PikaHubSenderThread::get_next(bool &is_error) {
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

unsigned int PikaHubSenderThread::ReadPhysicalRecord(slash::Slice *result) {
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
  s = queue_->Read(length, &buffer_, backing_store_);
  *result = slash::Slice(buffer_.data(), buffer_.size());
  last_record_offset_ += kHeaderSize + length;
  if (s.ok()) {
    con_offset_ += (kHeaderSize + length);
  }
  return type;
}

Status PikaHubSenderThread::Consume(std::string &scratch) {
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
Status PikaHubSenderThread::Parse(std::string &scratch) {
  scratch.clear();
  Status s;
  uint32_t pro_num;
  uint64_t pro_offset;

  while (!should_stop() && !need_reset_) {
    g_pika_server->logger_->GetProducerStatus(&pro_num, &pro_offset);
    if (filenum_ == pro_num && con_offset_ == pro_offset) {
      usleep(10000);
      continue;
    }

    s = Consume(scratch);

    if (s.IsEndFile()) {
      std::string confile = NewFileName(g_pika_server->logger_->filename, filenum_ + 1);

      // Roll to next File
      if (slash::FileExists(confile)) {
        bool should_wait = false;
        notify_manager_(this, filenum_, &should_wait);
        if (should_wait) {
          usleep(10000);
        }
      } else {
        usleep(10000);
      }
    } else if (scratch.size() > 3) {
      const char* send_to_hub = scratch.data() + scratch.size() - 3/* 1\r\n */;
      if (*send_to_hub == '1') {
        break; // Send this binlog
      } else {
        continue; // Next binlog
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
void* PikaHubSenderThread::ThreadMain() {
  Status s, result;
  bool last_send_flag = true;
  std::string scratch;
  scratch.reserve(1024 * 1024);

  while (!should_stop()) {
    if (need_reset_) {
      need_reset_ = false;
      reset_func_();
    }
    sleep(1);
    // 1. Connect to slave
    result = cli_->Connect(hub_ip_, hub_port_, g_pika_server->host());
    LOG(INFO) << "BinlogSender Connect slave(" << hub_ip_ << ":" <<
      hub_port_ << ") " << result.ToString();

    if (result.ok()) {
      cli_->set_send_timeout(timeout_ms_);
      while (true) {
        // 2. Should Parse new msg;
        if (last_send_flag) {
          s = Parse(scratch);

          if (s.IsCorruption()) {     // should exit
            LOG(WARNING) << "BinlogSender Parse failed, will exit, error: " << s.ToString();
            break;
          } else if (s.IsIOError()) {
            LOG(WARNING) << "BinlogSender Parse error, " << s.ToString();
            continue;
          } else if (need_reset_) {
            need_reset_ = false;
            reset_func_();
            DLOG(INFO) << "Reset hub binlog sender";
            break;
          }
        }

        // 3. After successful parse, we send msg;
        result = cli_->Send(&scratch);
        if (result.ok()) {
          last_send_flag = true;
        } else {
          last_send_flag = false;
          DLOG(INFO) << "BinlogSender send slave(" << hub_ip_ << ":" <<
            hub_port_ << ") failed,  " << result.ToString();
          break;
        }
      }
    }

    // error
    cli_->Close();
  }
  return nullptr;
}

PikaHubReceiverThread::PikaHubReceiverThread(const std::set<std::string> &ips, int port,
                                             int cron_interval)
      : conn_factory_(this),
        handles_(this) {
  cmds_.reserve(300);
  InitCmdTable(&cmds_);
  thread_rep_ = pink::NewHolyThread(ips, port, &conn_factory_,
                                    cron_interval, &handles_);
  thread_rep_->set_thread_name("HubReceiver");
  thread_rep_->set_keepalive_timeout(0);
}

PikaHubReceiverThread::~PikaHubReceiverThread() {
  thread_rep_->StopThread();
  LOG(INFO) << "BinlogReceiver thread " << thread_rep_->thread_id() << " exit!!!";
  delete thread_rep_;
}

int PikaHubReceiverThread::StartThread() {
  return thread_rep_->StartThread();
}

bool PikaHubReceiverThread::Handles::AccessHandle(std::string& ip) const {
  if (ip == "127.0.0.1") {
    ip = g_pika_server->host();
  }
  // if (hub_receiver_->thread_rep_->conn_num() != 0) {
  //     // !g_pika_server->IsHub(ip)) {
  //   LOG(WARNING) << "HubReceiverThread AccessHandle failed: " << ip;
  //   return false;
  // }
  g_pika_server->HubConnected();
  DLOG(INFO) << "hub connected: " << ip;
  return true;
}

void PikaHubReceiverThread::Handles::FdClosedHandle(
        int fd, const std::string& ip_port) const {
  LOG(INFO) << "HubReceiverThread Fd closed: " << ip_port;
  g_pika_server->StopHub();
}
