// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef NET_INCLUDE_PUBSUB_H_
#define NET_INCLUDE_PUBSUB_H_

#include <fcntl.h>
#include <atomic>
#include <functional>
#include <map>
#include <queue>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "pstd/include/pstd_mutex.h"
#include "pstd/include/pstd_string.h"
#include "pstd/include/xdebug.h"

#include "net/include/net_define.h"
#include "net/include/net_thread.h"
#include "net/src/net_multiplexer.h"

namespace net {

class NetFiredEvent;
class NetConn;

class PubSubThread : public Thread {
 public:
  PubSubThread();

  ~PubSubThread() override;

  // PubSub

  int Publish(const std::string& channel, const std::string& msg);

  void Subscribe(const std::shared_ptr<NetConn>& conn, const std::vector<std::string>& channels, bool pattern,
                 std::vector<std::pair<std::string, int>>* result);

  int UnSubscribe(const std::shared_ptr<NetConn>& conn, const std::vector<std::string>& channels, bool pattern,
                  std::vector<std::pair<std::string, int>>* result);

  void PubSubChannels(const std::string& pattern, std::vector<std::string>* result);

  void PubSubNumSub(const std::vector<std::string>& channels, std::vector<std::pair<std::string, int>>* result);

  int PubSubNumPat();

  // Move out from pubsub thread
  void MoveConnOut(const std::shared_ptr<NetConn>& conn);
  // Move into pubsub thread
  void MoveConnIn(const std::shared_ptr<NetConn>& conn, const NotifyType& notify_type);

  void ConnCanSubscribe(const std::vector<std::string>& allChannel,
                        const std::function<bool(const std::shared_ptr<NetConn>&)>& func);

  enum ReadyState {
    kNotReady,
    kReady,
  };

  struct ConnHandle {
    ConnHandle(std::shared_ptr<NetConn> pc, ReadyState state = kNotReady) : conn(std::move(pc)), ready_state(state) {}
    void UpdateReadyState(const ReadyState& state);
    bool IsReady();
    std::shared_ptr<NetConn> conn;
    ReadyState ready_state;
  };

  void UpdateConnReadyState(int fd, const ReadyState& state);

  bool IsReady(int fd);
  int ClientPubSubChannelSize(const std::shared_ptr<NetConn>& conn);
  int ClientPubSubChannelPatternSize(const std::shared_ptr<NetConn>& conn);

 private:
  void RemoveConn(const std::shared_ptr<NetConn>& conn);
  void CloseConn(const std::shared_ptr<NetConn>& conn);

  int ClientChannelSize(const std::shared_ptr<NetConn>& conn);

  int msg_pfd_[2];
  bool should_exit_;

  mutable pstd::RWMutex rwlock_; /* For external statistics */
  std::map<int, std::shared_ptr<ConnHandle>> conns_;

  pstd::Mutex pub_mutex_;
  pstd::CondVar receiver_rsignal_;
  pstd::Mutex receiver_mutex_;

  /*
   * receive fd from worker thread
   */
  pstd::Mutex mutex_;
  std::queue<NetItem> queue_;

  std::string channel_;
  std::string message_;
  int receivers_{-1};

  /*
   * The epoll handler
   */
  std::unique_ptr<NetMultiplexer> net_multiplexer_;

  void* ThreadMain() override;

  // clean conns
  void Cleanup();

  // PubSub
  pstd::Mutex channel_mutex_;
  pstd::Mutex pattern_mutex_;

  std::map<std::string, std::vector<std::shared_ptr<NetConn>>> pubsub_channel_;  // channel <---> conns
  std::map<std::string, std::vector<std::shared_ptr<NetConn>>> pubsub_pattern_;  // channel <---> conns

};  // class PubSubThread

}  // namespace net
#endif  // THIRD_NET_NET_INCLUDE_NET_PUBSUB_H_
