// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_MANAGER_H_
#define PIKA_MANAGER_H_

#include <string>
#include <queue>
#include <set>
#include <memory>
#include <future>
#include <sstream>

#include "pink/include/server_thread.h"
#include "pink/include/pink_thread.h"
#include "pink/include/pink_cli.h"
#include "slash/include/slash_slice.h"
#include "slash/include/slash_status.h"
#include "slash/include/env.h"
#include "slash/include/slash_mutex.h"
#include "include/pika_define.h"
#include "include/pika_command.h"

using slash::Status;
using slash::Slice;

const int kMaxHubSender = 10;

class PikaHubManager;

class PikaHubSenderThread : public pink::Thread {
 public:

  PikaHubSenderThread(int i, PikaHubManager* manager);
  virtual ~PikaHubSenderThread();

  int TryStartThread(const std::string& hub_ip, const int hub_port);

  int GetTid() { return tid_; }

  enum STATUS { UNSTARTED, WORKING, WAITING, ENDOFFILE };
  STATUS SenderStatus() { return status_; }

  void Reset() { should_reset_ = true; should_reconnect_ = true; }

 private:
  bool ResetStatus();
  int TrimOffset();

  uint64_t get_next(bool &is_error);
  Status Parse(std::string &scratch);
  Status Consume(std::string &scratch);
  unsigned int ReadPhysicalRecord(slash::Slice *fragment);

  PikaHubManager* pika_hub_manager_;

  STATUS status_;
  std::atomic<bool> should_reset_;
  std::atomic<bool> should_reconnect_;

  uint32_t filenum_;
  uint64_t con_offset_;
  uint64_t last_record_offset_;

  std::unique_ptr<slash::SequentialFile> queue_;
  char* const backing_store_;
  Slice buffer_;

  std::string hub_ip_;
  int hub_port_;

  int timeout_ms_;
  std::unique_ptr<pink::PinkCli> cli_;

  int tid_;
  virtual void* ThreadMain();
};

class PikaHubConn: public pink::RedisConn {
 public:
  PikaHubConn(int fd, std::string ip_port, CmdTable* cmds);
  virtual int DealMessage();

 private:
  CmdTable* cmds_;
};

class PikaHubReceiverThread {
 public:
  PikaHubReceiverThread(const std::set<std::string> &ips, int port, int cron_interval = 0);
  ~PikaHubReceiverThread();

  int StartThread();
  void KillAllConns() {
    thread_rep_->KillAllConns();
  }

 private:
  class HubConnFactory : public pink::ConnFactory {
   public:
    explicit HubConnFactory(PikaHubReceiverThread* hub_receiver)
        : hub_receiver_(hub_receiver) {
    }

    virtual pink::PinkConn *NewPinkConn(
        int connfd,
        const std::string &ip_port,
        pink::ServerThread *thread,
        void* worker_specific_data) const override {
      return new PikaHubConn(connfd, ip_port, &hub_receiver_->cmds_);
    }

   private:
    PikaHubReceiverThread* hub_receiver_;
  };

  class Handles : public pink::ServerHandle {
   public:
    explicit Handles(PikaHubReceiverThread* hub_receiver)
        : hub_receiver_(hub_receiver) {
    }

    using pink::ServerHandle::AccessHandle;
    bool AccessHandle(std::string& ip) const override;
    void FdClosedHandle(int fd, const std::string& ip) const override;

   private:
    PikaHubReceiverThread* hub_receiver_;
  };

  HubConnFactory conn_factory_;
  Handles handles_;
  pink::ServerThread* thread_rep_;

  int hub_connections_;

  CmdTable cmds_;
};

class PikaHubManager {
 public:
  PikaHubManager(const std::set<std::string> &ips, int port, int cron_interval = 0);
  int StartReceiver() {
    return hub_receiver_->StartThread();
  }

  Status AddHub(const std::string hub_ip, int hub_port,
                uint32_t filenum, uint64_t con_offset,
                bool send_most_recently);

  std::string hub_ip() { return hub_ip_; }

  bool CouldPurge(int file_tobe_purged) {
    slash::MutexLock l(&sending_window_protector_);
    return file_tobe_purged < sending_window_.left;
  }

  std::string StatusToString();

  void HubConnected() { hub_stage_ = STARTED; }
  void StopHub(int connnection_num);

 private:
  friend class PikaHubSenderThread;
  Status ResetSenders(bool send_most_recently);
  bool GetNextFilenum(PikaHubSenderThread* thread,
    uint32_t* filenum, uint64_t* con_offset);

  enum HUBSTAGE { STOPED, STARTING, DEGRADE, STARTED };
  slash::Mutex hub_stage_protector_;
  HUBSTAGE hub_stage_;
  std::string hub_ip_;
  int hub_port_;
  // which file I will send
  uint32_t hub_filenum_;
  uint64_t hub_con_offset_;

  slash::Mutex sending_window_protector_;
  struct {
    int64_t left;
    int64_t right;
  } sending_window_;

  std::unique_ptr<PikaHubSenderThread> sender_threads_[kMaxHubSender];
  std::map<PikaHubSenderThread*, uint32_t> working_map_;
  std::unique_ptr<PikaHubReceiverThread> hub_receiver_;
};

#endif
