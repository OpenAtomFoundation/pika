// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <algorithm>
#include <sstream>
#include <vector>

#include "net/src/worker_thread.h"

#include "net/include/net_conn.h"
#include "net/include/net_pubsub.h"
#include "net/src/net_item.h"

namespace net {

static std::string ConstructPublishResp(const std::string& subscribe_channel, const std::string& publish_channel,
                                        const std::string& msg, const bool pattern) {
  std::stringstream resp;
  std::string common_msg = "message";
  std::string pattern_msg = "pmessage";
  if (pattern) {
    resp << "*4\r\n"
         << "$" << pattern_msg.length() << "\r\n"
         << pattern_msg << "\r\n"
         << "$" << subscribe_channel.length() << "\r\n"
         << subscribe_channel << "\r\n"
         << "$" << publish_channel.length() << "\r\n"
         << publish_channel << "\r\n"
         << "$" << msg.length() << "\r\n"
         << msg << "\r\n";
  } else {
    resp << "*3\r\n"
         << "$" << common_msg.length() << "\r\n"
         << common_msg << "\r\n"
         << "$" << publish_channel.length() << "\r\n"
         << publish_channel << "\r\n"
         << "$" << msg.length() << "\r\n"
         << msg << "\r\n";
  }
  return resp.str();
}

void CloseFd(std::shared_ptr<NetConn> conn) { close(conn->fd()); }

void PubSubThread::ConnHandle::UpdateReadyState(const ReadyState& state) { ready_state = state; }

bool PubSubThread::ConnHandle::IsReady() { return ready_state == PubSubThread::ReadyState::kReady; }

PubSubThread::PubSubThread() : receivers_(-1) {
  set_thread_name("PubSubThread");
  net_multiplexer_.reset(CreateNetMultiplexer());
  net_multiplexer_->Initialize();
  if (pipe(msg_pfd_)) {
    exit(-1);
  }
  fcntl(msg_pfd_[0], F_SETFD, fcntl(msg_pfd_[0], F_GETFD) | FD_CLOEXEC);
  fcntl(msg_pfd_[1], F_SETFD, fcntl(msg_pfd_[1], F_GETFD) | FD_CLOEXEC);

  net_multiplexer_->NetAddEvent(msg_pfd_[0], kReadable);
}

PubSubThread::~PubSubThread() { StopThread(); }

void PubSubThread::MoveConnOut(std::shared_ptr<NetConn> conn) {
  RemoveConn(conn);

  net_multiplexer_->NetDelEvent(conn->fd(), 0);
  {
    std::lock_guard l(rwlock_);
    conns_.erase(conn->fd());
  }
}

void PubSubThread::MoveConnIn(std::shared_ptr<NetConn> conn, const NotifyType& notify_type) {
  NetItem it(conn->fd(), conn->ip_port(), notify_type);
  net_multiplexer_->Register(it, true);
  {
    std::lock_guard l(rwlock_);
    conns_[conn->fd()] = std::make_shared<ConnHandle>(conn);
  }
  conn->set_net_multiplexer(net_multiplexer_.get());
}

void PubSubThread::UpdateConnReadyState(int fd, const ReadyState& state) {
  std::lock_guard l(rwlock_);
  const auto& it = conns_.find(fd);
  if (it == conns_.end()) {
    return;
  }
  it->second->UpdateReadyState(state);
}

bool PubSubThread::IsReady(int fd) {
  std::shared_lock l(rwlock_);
  const auto& it = conns_.find(fd);
  if (it != conns_.end()) {
    return it->second->IsReady();
  }
  return false;
}

void PubSubThread::RemoveConn(std::shared_ptr<NetConn> conn) {
  {
    std::lock_guard lock(pattern_mutex_);
    for (auto it = pubsub_pattern_.begin(); it != pubsub_pattern_.end(); it++) {
      for (auto conn_ptr = it->second.begin(); conn_ptr != it->second.end(); conn_ptr++) {
        if ((*conn_ptr) == conn) {
          conn_ptr = it->second.erase(conn_ptr);
          break;
        }
      }
    }
  }

  {
    std::lock_guard lock(channel_mutex_);
    for (auto it = pubsub_channel_.begin(); it != pubsub_channel_.end(); it++) {
      for (auto conn_ptr = it->second.begin(); conn_ptr != it->second.end(); conn_ptr++) {
        if ((*conn_ptr) == conn) {
          conn_ptr = it->second.erase(conn_ptr);
          break;
        }
      }
    }
  }
}

int PubSubThread::Publish(const std::string& channel, const std::string& msg) {
  // TODO(LIBA-S): change the Publish Mode to Asynchronous
  std::lock_guard lk(pub_mutex_);
  channel_ = channel;
  message_ = msg;
  // Send signal to ThreadMain()
  write(msg_pfd_[1], "", 1);
  std::unique_lock lock(receiver_mutex_);
  receiver_rsignal_.wait(lock, [this]() { return receivers_ != -1; });

  int receivers = receivers_;
  receivers_ = -1;

  return receivers;
}

/*
 * return the number of channels that the specific connection currently subscribed
 */
int PubSubThread::ClientChannelSize(std::shared_ptr<NetConn> conn) {
  int subscribed = 0;

  channel_mutex_.lock();
  for (auto& channel : pubsub_channel_) {
    auto conn_ptr = std::find(channel.second.begin(), channel.second.end(), conn);
    if (conn_ptr != channel.second.end()) {
      subscribed++;
    }
  }
  channel_mutex_.unlock();

  pattern_mutex_.lock();
  for (auto& channel : pubsub_pattern_) {
    auto conn_ptr = std::find(channel.second.begin(), channel.second.end(), conn);
    if (conn_ptr != channel.second.end()) {
      subscribed++;
    }
  }
  pattern_mutex_.unlock();

  return subscribed;
}

void PubSubThread::Subscribe(std::shared_ptr<NetConn> conn, const std::vector<std::string>& channels,
                             const bool pattern, std::vector<std::pair<std::string, int>>* result) {
  int subscribed = ClientChannelSize(conn);

  if (subscribed == 0) {
    MoveConnIn(conn, net::NotifyType::kNotiWait);
  }

  for (size_t i = 0; i < channels.size(); i++) {
    if (pattern) {  // if pattern mode, register channel to map
      std::lock_guard channel_lock(pattern_mutex_);
      if (pubsub_pattern_.find(channels[i]) != pubsub_pattern_.end()) {
        auto conn_ptr = std::find(pubsub_pattern_[channels[i]].begin(), pubsub_pattern_[channels[i]].end(), conn);
        if (conn_ptr == pubsub_pattern_[channels[i]].end()) {  // the connection first subscrbied
          pubsub_pattern_[channels[i]].push_back(conn);
          ++subscribed;
        }
      } else {  // the channel first subscribed
        std::vector<std::shared_ptr<NetConn>> conns = {conn};
        pubsub_pattern_[channels[i]] = conns;
        ++subscribed;
      }
      result->push_back(std::make_pair(channels[i], subscribed));
    } else {  // if general mode, reigster channel to map
      std::lock_guard channel_lock(channel_mutex_);
      if (pubsub_channel_.find(channels[i]) != pubsub_channel_.end()) {
        auto conn_ptr = std::find(pubsub_channel_[channels[i]].begin(), pubsub_channel_[channels[i]].end(), conn);
        if (conn_ptr == pubsub_channel_[channels[i]].end()) {  // the connection first subscribed
          pubsub_channel_[channels[i]].push_back(conn);
          ++subscribed;
        }
      } else {  // the channel first subscribed
        std::vector<std::shared_ptr<NetConn>> conns = {conn};
        pubsub_channel_[channels[i]] = conns;
        ++subscribed;
      }
      result->push_back(std::make_pair(channels[i], subscribed));
    }
  }
}

/*
 * Unsubscribes the client from the given channels, or from all of them if none
 * is given.
 */
int PubSubThread::UnSubscribe(std::shared_ptr<NetConn> conn, const std::vector<std::string>& channels,
                              const bool pattern, std::vector<std::pair<std::string, int>>* result) {
  int subscribed = ClientChannelSize(conn);
  bool exist = true;
  if (subscribed == 0) {
    exist = false;
  }
  if (channels.size() == 0) {  // if client want to unsubscribe all of channels
    if (pattern) {             // all of pattern channels
      std::lock_guard l(pattern_mutex_);
      for (auto& channel : pubsub_pattern_) {
        auto conn_ptr = std::find(channel.second.begin(), channel.second.end(), conn);
        if (conn_ptr != channel.second.end()) {
          result->push_back(std::make_pair(channel.first, --subscribed));
        }
      }
    } else {
      std::lock_guard l(channel_mutex_);
      for (auto& channel : pubsub_channel_) {
        auto conn_ptr = std::find(channel.second.begin(), channel.second.end(), conn);
        if (conn_ptr != channel.second.end()) {
          result->push_back(std::make_pair(channel.first, --subscribed));
        }
      }
    }
    if (exist) {
      MoveConnOut(conn);
    }
    return 0;
  }

  for (size_t i = 0; i < channels.size(); i++) {
    if (pattern) {  // if pattern mode, unsubscribe the channels of specified
      std::lock_guard l(pattern_mutex_);
      auto channel_ptr = pubsub_pattern_.find(channels[i]);
      if (channel_ptr != pubsub_pattern_.end()) {
        auto it = std::find(channel_ptr->second.begin(), channel_ptr->second.end(), conn);
        if (it != channel_ptr->second.end()) {
          channel_ptr->second.erase(std::remove(channel_ptr->second.begin(), channel_ptr->second.end(), conn),
                                    channel_ptr->second.end());
          result->push_back(std::make_pair(channels[i], --subscribed));
        } else {
          result->push_back(std::make_pair(channels[i], subscribed));
        }
      } else {
        result->push_back(std::make_pair(channels[i], 0));
      }
    } else {  // if general mode, unsubscribe the channels of specified
      std::lock_guard l(channel_mutex_);
      auto channel_ptr = pubsub_channel_.find(channels[i]);
      if (channel_ptr != pubsub_channel_.end()) {
        auto it = std::find(channel_ptr->second.begin(), channel_ptr->second.end(), conn);
        if (it != channel_ptr->second.end()) {
          channel_ptr->second.erase(std::remove(channel_ptr->second.begin(), channel_ptr->second.end(), conn),
                                    channel_ptr->second.end());
          result->push_back(std::make_pair(channels[i], --subscribed));
        } else {
          result->push_back(std::make_pair(channels[i], subscribed));
        }
      } else {
        result->push_back(std::make_pair(channels[i], 0));
      }
    }
  }
  // The number of channels this client currently subscibred
  // include general mode and pattern mode
  subscribed = ClientChannelSize(conn);
  if (subscribed == 0 && exist) {
    MoveConnOut(conn);
  }
  return subscribed;
}

void PubSubThread::PubSubChannels(const std::string& pattern, std::vector<std::string>* result) {
  if (pattern == "") {
    std::lock_guard l(channel_mutex_);
    for (auto& channel : pubsub_channel_) {
      if (channel.second.size() != 0) {
        result->push_back(channel.first);
      }
    }
  } else {
    std::lock_guard l(channel_mutex_);
    for (auto& channel : pubsub_channel_) {
      if (pstd::stringmatchlen(channel.first.c_str(), channel.first.size(), pattern.c_str(), pattern.size(), 0)) {
        if (channel.second.size() != 0) {
          result->push_back(channel.first);
        }
      }
    }
  }
}

void PubSubThread::PubSubNumSub(const std::vector<std::string>& channels,
                                std::vector<std::pair<std::string, int>>* result) {
  int subscribed;
  std::lock_guard l(channel_mutex_);
  for (size_t i = 0; i < channels.size(); i++) {
    subscribed = 0;
    for (auto& channel : pubsub_channel_) {
      if (channel.first == channels[i]) {
        subscribed = channel.second.size();
      }
    }
    result->push_back(std::make_pair(channels[i], subscribed));
  }
}

int PubSubThread::PubSubNumPat() {
  int subscribed = 0;
  std::lock_guard l(pattern_mutex_);
  for (auto& channel : pubsub_pattern_) {
    subscribed += channel.second.size();
  }
  return subscribed;
}

void* PubSubThread::ThreadMain() {
  int nfds;
  NetFiredEvent* pfe;
  pstd::Status s;
  std::shared_ptr<NetConn> in_conn = nullptr;
  char triger[1];

  while (!should_stop()) {
    nfds = net_multiplexer_->NetPoll(NET_CRON_INTERVAL);
    for (int i = 0; i < nfds; i++) {
      pfe = (net_multiplexer_->FiredEvents()) + i;
      if (pfe->fd == net_multiplexer_->NotifyReceiveFd()) {  // New connection comming
        if (pfe->mask & kReadable) {
          read(net_multiplexer_->NotifyReceiveFd(), triger, 1);
          {
            NetItem ti = net_multiplexer_->NotifyQueuePop();
            if (ti.notify_type() == kNotiClose) {
            } else if (ti.notify_type() == kNotiEpollout) {
              net_multiplexer_->NetModEvent(ti.fd(), 0, kWritable);
            } else if (ti.notify_type() == kNotiEpollin) {
              net_multiplexer_->NetModEvent(ti.fd(), 0, kReadable);
            } else if (ti.notify_type() == kNotiEpolloutAndEpollin) {
              net_multiplexer_->NetModEvent(ti.fd(), 0, kWritable | kReadable);
            } else if (ti.notify_type() == kNotiWait) {
              // do not register events
              net_multiplexer_->NetAddEvent(ti.fd(), 0);
            }
          }
          continue;
        }
      }
      if (pfe->fd == msg_pfd_[0]) {  // Publish message
        if (pfe->mask & kReadable) {
          read(msg_pfd_[0], triger, 1);
          std::string channel, msg;
          int32_t receivers = 0;
          channel = channel_;
          msg = message_;
          channel_.clear();
          message_.clear();

          // Send message to clients
          channel_mutex_.lock();
          auto it = pubsub_channel_.find(channel);
          if(it != pubsub_channel_.end()){
              for (size_t i = 0; i < it->second.size(); i++) {
                if (!IsReady(it->second[i]->fd())) {
                  continue;
                }
                std::string resp = ConstructPublishResp(it->first, channel, msg, false);
                it->second[i]->WriteResp(resp);
                WriteStatus write_status = it->second[i]->SendReply();
                if (write_status == kWriteHalf) {
                  net_multiplexer_->NetModEvent(it->second[i]->fd(), kReadable, kWritable);
                } else if (write_status == kWriteError) {
                  channel_mutex_.unlock();

                  MoveConnOut(it->second[i]);

                  channel_mutex_.lock();
                  CloseFd(it->second[i]);
                } else if (write_status == kWriteAll) {
                  receivers++;
                }
              }
            }
          channel_mutex_.unlock();

          // Send message to clients
          pattern_mutex_.lock();
          for (auto it = pubsub_pattern_.begin(); it != pubsub_pattern_.end(); it++) {
            if (pstd::stringmatchlen(it->first.c_str(), it->first.size(), channel.c_str(), channel.size(), 0)) {
              for (size_t i = 0; i < it->second.size(); i++) {
                if (!IsReady(it->second[i]->fd())) {
                  continue;
                }
                std::string resp = ConstructPublishResp(it->first, channel, msg, true);
                it->second[i]->WriteResp(resp);
                WriteStatus write_status = it->second[i]->SendReply();
                if (write_status == kWriteHalf) {
                  net_multiplexer_->NetModEvent(it->second[i]->fd(), kReadable, kWritable);
                } else if (write_status == kWriteError) {
                  pattern_mutex_.unlock();

                  MoveConnOut(it->second[i]);

                  pattern_mutex_.lock();
                  CloseFd(it->second[i]);
                } else if (write_status == kWriteAll) {
                  receivers++;
                }
              }
            }
          }
          pattern_mutex_.unlock();

          receiver_mutex_.lock();
          receivers_ = receivers;
          receiver_rsignal_.notify_one();
          receiver_mutex_.unlock();
        } else {
          continue;
        }
      } else {
        in_conn = nullptr;
        bool should_close = false;

        {
          std::shared_lock l(rwlock_);
          if (auto iter = conns_.find(pfe->fd); iter == conns_.end()) {
            net_multiplexer_->NetDelEvent(pfe->fd, 0);
            continue;
          } else {
            in_conn = iter->second->conn;
          }
        }

        // Send reply
        if (pfe->mask & kWritable && in_conn->is_ready_to_reply()) {
          WriteStatus write_status = in_conn->SendReply();
          if (write_status == kWriteAll) {
            in_conn->set_is_reply(false);
            net_multiplexer_->NetModEvent(pfe->fd, 0, kReadable);  // Remove kWritable
          } else if (write_status == kWriteHalf) {
            continue;  //  send all write buffer,
                       //  in case of next GetRequest()
                       //  pollute the write buffer
          } else if (write_status == kWriteError) {
            should_close = true;
          }
        }

        // Client request again
        if (!should_close && pfe->mask & kReadable) {
          ReadStatus getRes = in_conn->GetRequest();
          // Do not response to client when we leave the pub/sub status here
          if (getRes != kReadAll && getRes != kReadHalf) {
            // kReadError kReadClose kFullError kParseError kDealError
            should_close = true;
          } else if (in_conn->is_ready_to_reply()) {
            WriteStatus write_status = in_conn->SendReply();
            if (write_status == kWriteAll) {
              in_conn->set_is_reply(false);
            } else if (write_status == kWriteHalf) {
              net_multiplexer_->NetModEvent(pfe->fd, kReadable, kWritable);
            } else if (write_status == kWriteError) {
              should_close = true;
            }
          } else {
            continue;
          }
        }
        // Error
        if ((pfe->mask & kErrorEvent) || should_close) {
          MoveConnOut(in_conn);
          CloseFd(in_conn);
          in_conn = nullptr;
        }
      }
    }
  }
  Cleanup();
  return nullptr;
}

void PubSubThread::Cleanup() {
  std::lock_guard l(rwlock_);
  for (auto& iter : conns_) {
    CloseFd(iter.second->conn);
  }
  conns_.clear();
}

};  // namespace net
