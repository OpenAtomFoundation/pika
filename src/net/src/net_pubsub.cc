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

void CloseFd(const std::shared_ptr<NetConn>& conn) { close(conn->fd()); }

void PubSubThread::ConnHandle::UpdateReadyState(const ReadyState& state) { ready_state = state; }

bool PubSubThread::ConnHandle::IsReady() { return ready_state == PubSubThread::ReadyState::kReady; }

PubSubThread::PubSubThread() {
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

void PubSubThread::MoveConnOut(const std::shared_ptr<NetConn>& conn) {
  RemoveConn(conn);

  net_multiplexer_->NetDelEvent(conn->fd(), 0);
  {
    std::lock_guard l(rwlock_);
    conns_.erase(conn->fd());
  }
}

void PubSubThread::MoveConnIn(const std::shared_ptr<NetConn>& conn, const NotifyType& notify_type) {
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

int PubSubThread::ClientPubSubChannelSize(const std::shared_ptr<NetConn>& conn) {
  int subscribed = 0;
  std::lock_guard l(channel_mutex_);
  for (auto& channel : pubsub_channel_) {
    auto conn_ptr = std::find(channel.second.begin(), channel.second.end(), conn);
    if (conn_ptr != channel.second.end()) {
      subscribed++;
    }
  }
  return subscribed;
}

int PubSubThread::ClientPubSubChannelPatternSize(const std::shared_ptr<NetConn>& conn) {
  int subscribed = 0;
  std::lock_guard l(pattern_mutex_);
  for (auto& channel : pubsub_pattern_) {
    auto conn_ptr = std::find(channel.second.begin(), channel.second.end(), conn);
    if (conn_ptr != channel.second.end()) {
      subscribed++;
    }
  }
  return subscribed;
}

void PubSubThread::RemoveConn(const std::shared_ptr<NetConn>& conn) {
  {
    std::lock_guard lock(pattern_mutex_);
    for (auto& it : pubsub_pattern_) {
      for (auto conn_ptr = it.second.begin(); conn_ptr != it.second.end(); conn_ptr++) {
        if ((*conn_ptr) == conn) {
          conn_ptr = it.second.erase(conn_ptr);
          break;
        }
      }
    }
  }

  {
    std::lock_guard lock(channel_mutex_);
    for (auto& it : pubsub_channel_) {
      for (auto conn_ptr = it.second.begin(); conn_ptr != it.second.end(); conn_ptr++) {
        if ((*conn_ptr) == conn) {
          conn_ptr = it.second.erase(conn_ptr);
          break;
        }
      }
    }
  }
}

void PubSubThread::CloseConn(const std::shared_ptr<NetConn>& conn) {
  CloseFd(conn);
  net_multiplexer_->NetDelEvent(conn->fd(), 0);
  {
    std::lock_guard l(rwlock_);
    conns_.erase(conn->fd());
  }
}

int PubSubThread::Publish(const std::string& channel, const std::string& msg) {
  // TODO(LIBA-S): change the Publish Mode to Asynchronous
  std::lock_guard lk(pub_mutex_);
  channel_ = channel;
  message_ = msg;
  // Send signal to ThreadMain()
  ssize_t n = write(msg_pfd_[1], "", 1);
  (void)(n);
  std::unique_lock lock(receiver_mutex_);
  receiver_rsignal_.wait(lock, [this]() { return receivers_ != -1; });

  int receivers = receivers_;
  receivers_ = -1;

  return receivers;
}

/*
 * return the number of channels that the specific connection currently subscribed
 */
int PubSubThread::ClientChannelSize(const std::shared_ptr<NetConn>& conn) {
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

void PubSubThread::Subscribe(const std::shared_ptr<NetConn>& conn, const std::vector<std::string>& channels,
                             const bool pattern, std::vector<std::pair<std::string, int>>* result) {
  int subscribed = ClientChannelSize(conn);

  if (subscribed == 0) {
    MoveConnIn(conn, net::NotifyType::kNotiWait);
  }

  for (const auto& channel : channels) {
    if (pattern) {  // if pattern mode, register channel to map
      std::lock_guard channel_lock(pattern_mutex_);
      if (pubsub_pattern_.find(channel) != pubsub_pattern_.end()) {
        auto conn_ptr = std::find(pubsub_pattern_[channel].begin(), pubsub_pattern_[channel].end(), conn);
        if (conn_ptr == pubsub_pattern_[channel].end()) {  // the connection first subscrbied
          pubsub_pattern_[channel].push_back(conn);
          ++subscribed;
        }
      } else {  // the channel first subscribed
        std::vector<std::shared_ptr<NetConn>> conns = {conn};
        pubsub_pattern_[channel] = conns;
        ++subscribed;
      }
      result->emplace_back(channel, subscribed);
    } else {  // if general mode, reigster channel to map
      std::lock_guard channel_lock(channel_mutex_);
      if (pubsub_channel_.find(channel) != pubsub_channel_.end()) {
        auto conn_ptr = std::find(pubsub_channel_[channel].begin(), pubsub_channel_[channel].end(), conn);
        if (conn_ptr == pubsub_channel_[channel].end()) {  // the connection first subscribed
          pubsub_channel_[channel].push_back(conn);
          ++subscribed;
        }
      } else {  // the channel first subscribed
        std::vector<std::shared_ptr<NetConn>> conns = {conn};
        pubsub_channel_[channel] = conns;
        ++subscribed;
      }
      result->emplace_back(channel, subscribed);
    }
  }
}

/*
 * Unsubscribes the client from the given channels, or from all of them if none
 * is given.
 */
int PubSubThread::UnSubscribe(const std::shared_ptr<NetConn>& conn, const std::vector<std::string>& channels,
                              const bool pattern, std::vector<std::pair<std::string, int>>* result) {
  int subscribed = ClientChannelSize(conn);
  bool exist = true;
  if (subscribed == 0) {
    exist = false;
  }
  if (channels.empty()) {  // if client want to unsubscribe all of channels
    if (pattern) {         // all of pattern channels
      std::lock_guard l(pattern_mutex_);
      for (auto& channel : pubsub_pattern_) {
        auto conn_ptr = std::find(channel.second.begin(), channel.second.end(), conn);
        if (conn_ptr != channel.second.end()) {
          result->emplace_back(channel.first, --subscribed);
        }
      }
    } else {
      std::lock_guard l(channel_mutex_);
      for (auto& channel : pubsub_channel_) {
        auto conn_ptr = std::find(channel.second.begin(), channel.second.end(), conn);
        if (conn_ptr != channel.second.end()) {
          result->emplace_back(channel.first, --subscribed);
        }
      }
    }
    if (exist) {
      MoveConnOut(conn);
    }
    return 0;
  }

  for (const auto& channel : channels) {
    if (pattern) {  // if pattern mode, unsubscribe the channels of specified
      std::lock_guard l(pattern_mutex_);
      auto channel_ptr = pubsub_pattern_.find(channel);
      if (channel_ptr != pubsub_pattern_.end()) {
        auto it = std::find(channel_ptr->second.begin(), channel_ptr->second.end(), conn);
        if (it != channel_ptr->second.end()) {
          channel_ptr->second.erase(std::remove(channel_ptr->second.begin(), channel_ptr->second.end(), conn),
                                    channel_ptr->second.end());
          result->emplace_back(channel, --subscribed);
        } else {
          result->emplace_back(channel, subscribed);
        }
      } else {
        result->emplace_back(channel, 0);
      }
    } else {  // if general mode, unsubscribe the channels of specified
      std::lock_guard l(channel_mutex_);
      auto channel_ptr = pubsub_channel_.find(channel);
      if (channel_ptr != pubsub_channel_.end()) {
        auto it = std::find(channel_ptr->second.begin(), channel_ptr->second.end(), conn);
        if (it != channel_ptr->second.end()) {
          channel_ptr->second.erase(std::remove(channel_ptr->second.begin(), channel_ptr->second.end(), conn),
                                    channel_ptr->second.end());
          result->emplace_back(channel, --subscribed);
        } else {
          result->emplace_back(channel, subscribed);
        }
      } else {
        result->emplace_back(channel, 0);
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
  if (pattern.empty()) {
    std::lock_guard l(channel_mutex_);
    for (auto& channel : pubsub_channel_) {
      if (!channel.second.empty()) {
        result->push_back(channel.first);
      }
    }
  } else {
    std::lock_guard l(channel_mutex_);
    for (auto& channel : pubsub_channel_) {
      if (pstd::stringmatchlen(channel.first.c_str(), static_cast<int32_t>(channel.first.size()), pattern.c_str(),
                               static_cast<int32_t>(pattern.size()), 0)) {
        if (!channel.second.empty()) {
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
  for (const auto& i : channels) {
    subscribed = 0;
    for (auto& channel : pubsub_channel_) {
      if (channel.first == i) {
        subscribed = static_cast<int32_t>(channel.second.size());
      }
    }
    result->emplace_back(i, subscribed);
  }
}

int PubSubThread::PubSubNumPat() {
  int subscribed = 0;
  std::lock_guard l(pattern_mutex_);
  for (auto& channel : pubsub_pattern_) {
    subscribed += static_cast<int32_t>(channel.second.size());
  }
  return subscribed;
}

void PubSubThread::ConnCanSubscribe(const std::vector<std::string>& allChannel,
                                    const std::function<bool(const std::shared_ptr<NetConn>&)>& func) {
  {
    std::lock_guard l(channel_mutex_);
    for (auto& item : pubsub_channel_) {
      for (auto it = item.second.rbegin(); it != item.second.rend(); it++) {
        if (func(*it) && (allChannel.empty() || !std::count(allChannel.begin(), allChannel.end(), item.first))) {
          item.second.erase(std::next(it).base());
          CloseConn(*it);
        }
      }  // for end
    }
  }

  {
    std::lock_guard l(pattern_mutex_);
    for (auto& item : pubsub_pattern_) {
      for (auto it = item.second.rbegin(); it != item.second.rend(); it++) {
        bool kill = false;
        if (func(*it)) {
          if (allChannel.empty()) {
            kill = true;
          }
          for (const auto& channelName : allChannel) {
            if (kill || !pstd::stringmatchlen(channelName.c_str(), static_cast<int32_t>(channelName.size()),
                                              item.first.c_str(), static_cast<int32_t>(item.first.size()), 0)) {
              kill = true;
              break;
            }
          }
        }
        if (kill) {
          item.second.erase(std::next(it).base());
          CloseConn(*it);
        }
      }
    }
  }
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
          ssize_t n = read(net_multiplexer_->NotifyReceiveFd(), triger, 1);
          (void)(n);
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
          ssize_t n = read(msg_pfd_[0], triger, 1);
          (void)(n);
          std::string channel;
          std::string msg;
          int32_t receivers = 0;
          channel = channel_;
          msg = message_;
          channel_.clear();
          message_.clear();

          // Send message to a channel's clients
          channel_mutex_.lock();
          auto it = pubsub_channel_.find(channel);
          if (it != pubsub_channel_.end()) {
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

          // Send message to a channel pattern's clients
          pattern_mutex_.lock();
          for (auto& it : pubsub_pattern_) {
            if (pstd::stringmatchlen(it.first.c_str(), static_cast<int32_t>(it.first.size()), channel.c_str(),
                                     static_cast<int32_t>(channel.size()), 0)) {
              for (size_t i = 0; i < it.second.size(); i++) {
                if (!IsReady(it.second[i]->fd())) {
                  continue;
                }
                std::string resp = ConstructPublishResp(it.first, channel, msg, true);
                it.second[i]->WriteResp(resp);
                WriteStatus write_status = it.second[i]->SendReply();
                if (write_status == kWriteHalf) {
                  net_multiplexer_->NetModEvent(it.second[i]->fd(), kReadable, kWritable);
                } else if (write_status == kWriteError) {
                  pattern_mutex_.unlock();

                  MoveConnOut(it.second[i]);

                  pattern_mutex_.lock();
                  CloseFd(it.second[i]);
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
        if ((pfe->mask & kWritable) && in_conn->is_ready_to_reply()) {
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
        if (!should_close && (pfe->mask & kReadable)) {
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
