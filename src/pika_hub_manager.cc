// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_hub_manager.h"

#include <glog/logging.h>
#include <poll.h>
#include <unistd.h>
#include <sys/syscall.h>

#include "include/pika_server.h"
#include "include/pika_define.h"
#include "pink/include/redis_cli.h"

extern PikaServer* g_pika_server;

PikaHubManager::PikaHubManager(const std::set<std::string> &ips, int port,
                               int cron_interval)
    : hub_stage_(STOPED),
      hub_filenum_(0),
      hub_con_offset_(0),
      sending_window_({0, 0}),
      hub_receiver_(new PikaHubReceiverThread(ips, port, cron_interval)){
  for (int i = 0; i < kMaxHubSender; i++) {
    sender_threads_[i].reset(new PikaHubSenderThread(i, this));
  }
}

Status PikaHubManager::AddHub(const std::string hub_ip, int hub_port,
                                uint32_t filenum, uint64_t con_offset) {
  std::string ip_port = slash::IpPortString(hub_ip, hub_port);
  LOG(INFO) << "Try add hub, " << ip_port;

  {
  slash::MutexLock l(&hub_stage_protector_);

  if (hub_stage_ == STOPED) {
    // need start
  } else if (hub_stage_ == STARTING) {
    // we handle only one request
    return Status::OK();
  } else if (hub_stage_ == DEGRADE) {
    // kill the left connection
    hub_receiver_->KillAllConns();
  } else if (hub_stage_ == STARTED && 
             filenum != hub_filenum_ && con_offset != hub_con_offset_) {
    // need reset
  } else {
    // already exist
    return Status::OK();
  }

  hub_filenum_ = filenum;
  hub_con_offset_ = con_offset;
  hub_stage_ = STARTING;
  } // Unlock
  
  // start or reset senders
  hub_ip_ = hub_ip;
  hub_port_ = hub_port;

  Status s = ResetSenders();
  hub_stage_ = s.ok() ? READY : STOPED;
  return s;
}

Status PikaHubManager::ResetSenders() {
  assert(hub_stage_ < READY);
  // Sanitize
  uint32_t cur_filenum = 0;
  uint64_t cur_offset = 0;
  g_pika_server->logger_->GetProducerStatus(&cur_filenum, &cur_offset);
  if (hub_con_offset_ > g_pika_server->logger_->file_size() ||
      cur_filenum < hub_filenum_ ||
      (cur_filenum == hub_filenum_ && cur_offset < hub_con_offset_)) {
    return Status::InvalidArgument("AddHubBinlogSender invalid binlog offset");
  }
  std::string confile = NewFileName(g_pika_server->logger_->filename, hub_filenum_);
  if (!slash::FileExists(confile)) {
    // Not found binlog specified by filenum
    return Status::InvalidArgument("AddHubBinlogSender file does not exist");
  }

  for (int i = 0; i < kMaxHubSender; i++) {
    if (sender_threads_[i]->TryStartThread(hub_ip_, hub_port_) != 0) {
      LOG(ERROR) << "Start hub sender failed!";
      return Status::Corruption("Start hub sender failed!");
    }
  }

  // Init status, if cur_filenum equal second + 1, there is new binlog files
  slash::MutexLock l(&sending_window_protector_);
  sending_window_.left = static_cast<int64_t>(hub_filenum_);
  sending_window_.right = static_cast<int64_t>(hub_filenum_) - 1;

  LOG(INFO) << "Reset hub sending window: " <<
    sending_window_.left << "-" << sending_window_.right;

  // Clean working map
  working_map_.clear();

  return Status::OK();
}

// Will be invoded in PikaHubSenderThread
bool PikaHubManager::GetNextFilenum(PikaHubSenderThread* thread,
                                    uint32_t* filenum, uint64_t* con_offset) {
  bool should_wait = true;
  if (hub_stage_ < READY) {
    // STOPED, STARTING, DEGRADE
    return should_wait;
  }

  uint32_t cur_filenum;
  uint64_t cur_offset;
  g_pika_server->logger_->GetProducerStatus(&cur_filenum, &cur_offset);

  slash::MutexLock l(&sending_window_protector_);

  auto record = working_map_.find(thread);
  if (static_cast<int64_t>(cur_filenum) > sending_window_.right) {
    uint32_t new_filenum;
    // There is new file to assign
    if (record == working_map_.end()) {
      // New thread
      new_filenum = ++sending_window_.right;
      working_map_.insert(std::make_pair(thread, new_filenum));

      should_wait = false;
    } else if (record->second == sending_window_.left) {
      // Left window has finished
      sending_window_.left++;

      new_filenum = ++sending_window_.right;
      working_map_[thread] = new_filenum;

      should_wait = false;
    } else {
      // Middle has finished, should wait
    }

    if (!should_wait) {
      // Assign a new file
      *filenum = new_filenum;
      *con_offset = 0;

      DLOG(INFO) << "Hub sending window: " <<
        sending_window_.left << "-" << sending_window_.right;
      DLOG(INFO) << "Hub senders' map: ";
      for (auto& info : working_map_) {
        DLOG(INFO) << "    ---- Thread " << info.first <<
          " processing " << info.second;
      }
    }
  }

  return should_wait;
}

std::string PikaHubManager::StatusToString() {
  std::stringstream tmp_stream;
  const std::string CRLF = "\r\n";

  tmp_stream << "sending window: " <<
    sending_window_.left << " - " << sending_window_.right << CRLF;
  tmp_stream << "hub senders' map: " << CRLF;
  for (auto& info : working_map_) {
    std::string status;
    auto sender = info.first;
    switch(sender->SenderStatus()) {
      case PikaHubSenderThread::UNSTARTED:
        status.assign("UNSTARTED");
        break;
      case PikaHubSenderThread::WORKING:
        status.assign("WORKING");
        break;
      case PikaHubSenderThread::WAITING:
        status.assign("WAITING");
        break;
      case PikaHubSenderThread::ENDOFFILE:
        status.assign("ENDOFFILE");
        break;
      default:
        status.assign("UNKNOW");
        break;
    }
    tmp_stream << "    --- Thread " <<
      sender->GetTid() << ": " << info.second << " " << status << CRLF;
  }
  std::string stage;
  switch(hub_stage_) {
    case STOPED:
      stage.assign("STOPED");
      break;
    case STARTING:
      stage.assign("STARTING");
      break;
    case DEGRADE:
      stage.assign("DEGRADE");
      break;
    case READY:
      stage.assign("READY");
      break;
    case STARTED:
      stage.assign("STARTED");
      break;
  }
  tmp_stream << "hub stage: " << stage << CRLF;

  return tmp_stream.str();
}

// Hub sender thread

int PikaHubSenderThread::TryStartThread(const std::string& hub_ip,
                                        const int hub_port) {
  // Set hub info
  hub_ip_ = hub_ip;
  hub_port_ = hub_port;
  should_reset_ = true;

  return Thread::StartThread();
}

bool PikaHubSenderThread::ResetStatus() {
  bool should_wait = pika_hub_manager_->GetNextFilenum(this, &filenum_, &con_offset_);
  if (should_wait) {
    return true;
  }

  std::string confile = NewFileName(g_pika_server->logger_->filename, filenum_);
  if (!slash::FileExists(confile)) {
    // Not found binlog specified by filenum
    LOG(ERROR) << "File does not exist: " << filenum_;
    set_should_stop(true);
    return false;
  }

  slash::SequentialFile* readfile = nullptr;
  if (!slash::NewSequentialFile(confile, &readfile).ok()) {
    LOG(ERROR) << "AddHubBinlogSender new sequtialfile failed";
    set_should_stop(true);
    return false;
  }
  queue_.reset(readfile);

  if (TrimOffset() != 0) {
    LOG(ERROR) << "Trim failed";
    set_should_stop(true);
    return false;
  }

  return false;
}

PikaHubSenderThread::PikaHubSenderThread(int i, PikaHubManager* manager)
    : pika_hub_manager_(manager),
      status_(UNSTARTED),
      should_reset_(false),
      backing_store_(new char[kBlockSize]),
      buffer_(),
      timeout_ms_(35000),
      tid_(0) {
  cli_.reset(pink::NewRedisCli());
  set_thread_name("HubBinlogSender" + std::to_string(i));
}

PikaHubSenderThread::~PikaHubSenderThread() {
  StopThread();
  delete[] backing_store_;
  LOG(INFO) << "a BinlogSender thread " << thread_id() << " exit!";
}

int PikaHubSenderThread::TrimOffset() {
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

  while (!should_stop()) {
    g_pika_server->logger_->GetProducerStatus(&pro_num, &pro_offset);
    if (filenum_ != pro_num || con_offset_ != pro_offset) {
      status_ = WORKING;
      s = Consume(scratch);
    } else {
      sleep(1);
      status_ = WAITING;
    }

    // Notify manager to get next File
    if (should_reset_ || s.IsEndFile()) {
      status_ = ENDOFFILE;
      bool should_wait = ResetStatus();
      if (should_wait) {
        sleep(1);
      } else {
        should_reset_ = false;
        status_ = WORKING;
      }
    } else if (s.ok() && scratch.size() > 3) {
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

  // Assign tid_
  tid_ = syscall(SYS_gettid);

  bool should_wait = true;
  while (!should_stop()) {
    should_wait = ResetStatus();
    if (should_wait) {
      status_ = UNSTARTED;
      sleep(1);
      continue;
    } else {
      status_ = WORKING;
      should_reset_ = false;
    }
    // 1. Connect to slave
    result = cli_->Connect(hub_ip_, hub_port_, g_pika_server->host());
    LOG(INFO) << "Hub Sender Connect hub(" << hub_ip_ << ":" <<
      hub_port_ << ") " << result.ToString();

    if (result.ok()) {
      cli_->set_send_timeout(timeout_ms_);
      // Binlog sending loop
      while (!should_reset_) {
        // 2. Should Parse new msg;
        if (last_send_flag) {
          s = Parse(scratch);

          if (s.IsCorruption()) {     // should exit
            LOG(WARNING) << "Hub Sender Parse failed, will exit, error: " << s.ToString();
            break;
          } else if (s.IsIOError()) {
            LOG(WARNING) << "Hub Sender Parse error, " << s.ToString();
            continue;
          }
        }

        // 3. After successful parse, we send msg;
        result = cli_->Send(&scratch);
        if (result.ok()) {
          last_send_flag = true;
        } else {
          last_send_flag = false;
          DLOG(INFO) << "Hub Sender send hub(" << hub_ip_ << ":" <<
            hub_port_ << ") failed,  " << result.ToString();
          break;
        }
      }
    }

    // connect error
    cli_->Close();
  }
  return nullptr;
}

// Hub receiver

PikaHubReceiverThread::PikaHubReceiverThread(const std::set<std::string> &ips,
                                             int port, int cron_interval)
      : conn_factory_(this),
        handles_(this),
        hub_connections_(0) {
  cmds_.reserve(300);
  InitCmdTable(&cmds_);
  thread_rep_ = pink::NewHolyThread(ips, port, &conn_factory_,
                                    cron_interval, &handles_),
  thread_rep_->set_thread_name("HubReceiver");
  thread_rep_->set_keepalive_timeout(0);
}

PikaHubReceiverThread::~PikaHubReceiverThread() {
  thread_rep_->StopThread();
  LOG(INFO) << "HubReceiver thread " << thread_rep_->thread_id() << " exit!!!";
  delete thread_rep_;
}

int PikaHubReceiverThread::StartThread() {
  return thread_rep_->StartThread();
}

bool PikaHubReceiverThread::Handles::AccessHandle(std::string& ip) const {
  if (ip == "127.0.0.1") {
    ip = g_pika_server->host();
  }
  if (hub_receiver_->hub_connections_ == 2) {
    LOG(WARNING) << "HubReceiverThread AccessHandle failed: " << ip;
    return false;
  }
  if (++(hub_receiver_->hub_connections_) == 2) {
    g_pika_server->pika_hub_manager_->HubConnected();
    DLOG(INFO) << "hub connected: " << ip;
  }
  return true;
}

void PikaHubReceiverThread::Handles::FdClosedHandle(
        int fd, const std::string& ip_port) const {
  LOG(INFO) << "HubReceiverThread Fd closed: " << ip_port <<
    ", connections: " << hub_receiver_->hub_connections_;
  g_pika_server->pika_hub_manager_->StopHub(--hub_receiver_->hub_connections_);
}
