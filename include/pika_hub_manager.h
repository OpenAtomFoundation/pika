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

class PikaHubSenderThread : public pink::Thread {
 public:

  PikaHubSenderThread();
  virtual ~PikaHubSenderThread();

  int trim();
  void SetNotifier(std::function<void(PikaHubSenderThread*, int, bool*)>&& f) {
    notify_manager_ = f;
  }
  std::future<Status> Reset(uint32_t filenum, const std::string& hub_ip_,
                           const int hub_port_);

 private:
  uint64_t get_next(bool &is_error);
  Status Parse(std::string &scratch);
  Status Consume(std::string &scratch);
  unsigned int ReadPhysicalRecord(slash::Slice *fragment);

  std::promise<Status> reset_result_;
  std::atomic<bool> need_reset_;
  std::function<void()> reset_func_;
  std::function<void(PikaHubSenderThread*, int, bool*)> notify_manager_;

  uint64_t con_offset_;
  uint32_t filenum_;

  uint64_t last_record_offset_;

  std::unique_ptr<slash::SequentialFile> queue_;
  char* const backing_store_;
  Slice buffer_;

  std::string hub_ip_;
  int hub_port_;

  int timeout_ms_;
  std::unique_ptr<pink::PinkCli> cli_;

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

    bool AccessHandle(std::string& ip) const override;
    void FdClosedHandle(int fd, const std::string& ip) const override;

   private:
    PikaHubReceiverThread* hub_receiver_;
  };

  HubConnFactory conn_factory_;
  Handles handles_;
  pink::ServerThread* thread_rep_;

  CmdTable cmds_;
};

class PikaHubManager {
 public:
  PikaHubManager(const std::set<std::string> &ips, int port, int cron_interval = 0);
  int StartReceiver() {
    return hub_receiver_->StartThread();
  }

  Status AddHub(const std::string hub_ip, int hub_port,
                uint32_t filenum, uint64_t con_offset);

  void HubConnected() {
    hub_stage_ = STARTED;
  }
  void StopHub() {
    hub_stage_ = UNSTARTED;
  }

 private:
  Status ResetSenders();
  void Notifier(PikaHubSenderThread* thread, uint32_t filenum, bool* should_wait);

  enum HUBSTAGE { UNSTARTED, STARTING, STARTED };
  slash::Mutex hub_mutex_;
  HUBSTAGE hub_stage_;
  std::string hub_ip_;
  int hub_port_;
  // which file I will send
  uint32_t filenum_;
  uint64_t con_offset_;
  std::pair<uint32_t, uint32_t> sending_window_;

  std::unique_ptr<PikaHubSenderThread> sender_threads_[kMaxHubSender];
  std::unique_ptr<PikaHubReceiverThread> hub_receiver_;
};

#endif
