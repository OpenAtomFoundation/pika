// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <vector>
#include <algorithm>
#include <sstream>

#include "pink/src/worker_thread.h"

#include "pink/include/pink_conn.h"
#include "pink/src/pink_item.h"
#include "pink/src/pink_epoll.h"
#include "pink/include/pink_pubsub.h"

namespace pink {

static std::string ConstructPublishResp(const std::string& subscribe_channel,
                           const std::string& publish_channel,
                           const std::string& msg,
                           const bool pattern) {
  std::stringstream resp;
  std::string common_msg = "message";
  std::string pattern_msg = "pmessage";
  if (pattern) {
    resp << "*4\r\n" << "$" << pattern_msg.length()       << "\r\n" << pattern_msg       << "\r\n" <<
                        "$" << subscribe_channel.length() << "\r\n" << subscribe_channel << "\r\n" <<
                        "$" << publish_channel.length()   << "\r\n" << publish_channel   << "\r\n" <<
                        "$" << msg.length()               << "\r\n" << msg               << "\r\n";
  } else {
    resp << "*3\r\n" << "$" << common_msg.length()        << "\r\n" << common_msg        << "\r\n" <<
                        "$" << publish_channel.length()   << "\r\n" << publish_channel   << "\r\n" <<
                        "$" << msg.length()               << "\r\n" << msg               << "\r\n";
  }
  return resp.str();
}

void CloseFd(std::shared_ptr<PinkConn> conn) {
  close(conn->fd());
}

void PubSubThread::ConnHandle::UpdateReadyState(const ReadyState& state) {
  ready_state = state;
}

bool PubSubThread::ConnHandle::IsReady() {
  return ready_state == PubSubThread::ReadyState::kReady;
}

PubSubThread::PubSubThread()
      : receiver_rsignal_(&receiver_mutex_),
        receivers_(-1)  {
  set_thread_name("PubSubThread");
  pink_epoll_ = new PinkEpoll();
  if (pipe(msg_pfd_)) {
    exit(-1);
  }
  fcntl(msg_pfd_[0], F_SETFD, fcntl(msg_pfd_[0], F_GETFD) | FD_CLOEXEC);
  fcntl(msg_pfd_[1], F_SETFD, fcntl(msg_pfd_[1], F_GETFD) | FD_CLOEXEC);

  pink_epoll_->PinkAddEvent(msg_pfd_[0], EPOLLIN | EPOLLERR | EPOLLHUP);
}

PubSubThread::~PubSubThread() {
  StopThread();
  delete(pink_epoll_);
}

void PubSubThread::MoveConnOut(std::shared_ptr<PinkConn> conn) {
  RemoveConn(conn);

  pink_epoll_->PinkDelEvent(conn->fd());
  {
    slash::WriteLock l(&rwlock_);
    conns_.erase(conn->fd());
  }
}

void PubSubThread::MoveConnIn(std::shared_ptr<PinkConn> conn, const NotifyType& notify_type) {
  PinkItem it(conn->fd(), conn->ip_port(), notify_type);
  pink_epoll_->Register(it, true);
  {
    slash::WriteLock l(&rwlock_);
    conns_[conn->fd()] = std::make_shared<ConnHandle>(conn);
  }
  conn->set_pink_epoll(pink_epoll_);
}

void PubSubThread::UpdateConnReadyState(int fd, const ReadyState& state) {
  slash::WriteLock l(&rwlock_);
  const auto& it = conns_.find(fd);
  if (it == conns_.end()) {
    return;
  }
  it->second->UpdateReadyState(state);
}

bool PubSubThread::IsReady(int fd) {
  slash::ReadLock l(&rwlock_);
  const auto& it = conns_.find(fd);
  if (it != conns_.end()) {
    return it->second->IsReady();
  }
  return false;
}

void PubSubThread::RemoveConn(std::shared_ptr<PinkConn> conn) {
  pattern_mutex_.Lock();
  for (auto it = pubsub_pattern_.begin(); it != pubsub_pattern_.end(); it++) {
    for (auto conn_ptr = it->second.begin();
              conn_ptr != it->second.end();
              conn_ptr++) {
      if ((*conn_ptr) == conn) {
        conn_ptr = it->second.erase(conn_ptr);
        break;
      }
    }
  }
  pattern_mutex_.Unlock();

  channel_mutex_.Lock();
  for (auto it = pubsub_channel_.begin(); it != pubsub_channel_.end(); it++) {
    for (auto conn_ptr = it->second.begin();
              conn_ptr != it->second.end();
              conn_ptr++) {
      if ((*conn_ptr) == conn) {
        conn_ptr = it->second.erase(conn_ptr);
        break;
      }
    }
  }
  channel_mutex_.Unlock();
}

int PubSubThread::Publish(const std::string& channel, const std::string &msg) {
  // TODO(LIBA-S): change the Publish Mode to Asynchronous
  int receivers;
  pub_mutex_.Lock();

  channel_ = channel;
  message_ = msg;
  // Send signal to ThreadMain()
  write(msg_pfd_[1], "", 1);
  receiver_mutex_.Lock();
  while (receivers_ == -1) {
    receiver_rsignal_.Wait();
  }
  receivers = receivers_;
  receivers_ = -1;
  receiver_mutex_.Unlock();

  pub_mutex_.Unlock();
  return receivers;
}

/*
 * return the number of channels that the specific connection currently subscribed
 */
int PubSubThread::ClientChannelSize(std::shared_ptr<PinkConn> conn) {
  int subscribed = 0;

  channel_mutex_.Lock();
  for (auto& channel : pubsub_channel_) {
    auto conn_ptr = std::find(channel.second.begin(),
                              channel.second.end(),
                              conn);
    if (conn_ptr != channel.second.end()) {
      subscribed++;
    }
  }
  channel_mutex_.Unlock();

  pattern_mutex_.Lock();
  for (auto& channel : pubsub_pattern_) {
    auto conn_ptr = std::find(channel.second.begin(),
                              channel.second.end(),
                              conn);
    if (conn_ptr != channel.second.end()) {
      subscribed++;
    }
  }
  pattern_mutex_.Unlock();

  return subscribed;
}

void PubSubThread::Subscribe(std::shared_ptr<PinkConn> conn,
                             const std::vector<std::string>& channels,
                             const bool pattern,
                             std::vector<std::pair<std::string, int>>* result) {
  int subscribed = ClientChannelSize(conn);

  if (subscribed == 0) {
    MoveConnIn(conn, pink::NotifyType::kNotiWait);
  }

  for (size_t i = 0; i < channels.size(); i++) {
    if (pattern) {  // if pattern mode, register channel to map
      slash::MutexLock channel_lock(&pattern_mutex_);
      if (pubsub_pattern_.find(channels[i]) != pubsub_pattern_.end()) {
        auto conn_ptr = std::find(pubsub_pattern_[channels[i]].begin(),
                                  pubsub_pattern_[channels[i]].end(),
                                  conn);
        if (conn_ptr == pubsub_pattern_[channels[i]].end()) {   // the connection first subscrbied
          pubsub_pattern_[channels[i]].push_back(conn);
          ++subscribed;
        }
      } else {    // the channel first subscribed
        std::vector<std::shared_ptr<PinkConn> > conns = {conn};
        pubsub_pattern_[channels[i]] = conns;
        ++subscribed;
      }
      result->push_back(std::make_pair(channels[i], subscribed));
    } else {    // if general mode, reigster channel to map
      slash::MutexLock channel_lock(&channel_mutex_);
      if (pubsub_channel_.find(channels[i]) != pubsub_channel_.end()) {
        auto conn_ptr = std::find(pubsub_channel_[channels[i]].begin(),
                                  pubsub_channel_[channels[i]].end(),
                                  conn);
        if (conn_ptr == pubsub_channel_[channels[i]].end()) {   // the connection first subscribed
          pubsub_channel_[channels[i]].push_back(conn);
          ++subscribed;
        }
      } else {    // the channel first subscribed
        std::vector<std::shared_ptr<PinkConn> > conns = {conn};
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
int PubSubThread::UnSubscribe(std::shared_ptr<PinkConn> conn,
                              const std::vector<std::string>& channels,
                              const bool pattern,
                              std::vector<std::pair<std::string, int>>* result) {
  int subscribed = ClientChannelSize(conn);
  bool exist = true;
  if (subscribed == 0) {
    exist = false;
  }
  if (channels.size() == 0) {       // if client want to unsubscribe all of channels
    if (pattern) {                  // all of pattern channels
      slash::MutexLock l(&pattern_mutex_);
      for (auto& channel : pubsub_pattern_) {
        auto conn_ptr = std::find(channel.second.begin(),
                                channel.second.end(),
                                conn);
        if (conn_ptr != channel.second.end()) {
          result->push_back(std::make_pair(channel.first, --subscribed));
        }
      }
    } else {
      slash::MutexLock l(&channel_mutex_);
      for (auto& channel : pubsub_channel_) {
        auto conn_ptr = std::find(channel.second.begin(),
                                channel.second.end(),
                                conn);
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
    if (pattern) {      // if pattern mode, unsubscribe the channels of specified
      slash::MutexLock l(&pattern_mutex_);
      auto channel_ptr = pubsub_pattern_.find(channels[i]);
      if (channel_ptr != pubsub_pattern_.end()) {
        auto it = std::find(channel_ptr->second.begin(),
                            channel_ptr->second.end(),
                            conn);
        if (it != channel_ptr->second.end()) {
          channel_ptr->second.erase(std::remove(
                                                channel_ptr->second.begin(),
                                                channel_ptr->second.end(),
                                                conn),
                                      channel_ptr->second.end());
          result->push_back(std::make_pair(channels[i], --subscribed));
        } else {
          result->push_back(std::make_pair(channels[i], subscribed));
        }
      } else {
        result->push_back(std::make_pair(channels[i], 0));
      }
    } else {            // if general mode, unsubscribe the channels of specified
      slash::MutexLock l(&channel_mutex_);
      auto channel_ptr = pubsub_channel_.find(channels[i]);
      if (channel_ptr != pubsub_channel_.end()) {
        auto it = std::find(channel_ptr->second.begin(),
                            channel_ptr->second.end(),
                            conn);
        if (it != channel_ptr->second.end()) {
          channel_ptr->second.erase(std::remove(
                                  channel_ptr->second.begin(),
                                  channel_ptr->second.end(),
                                  conn),
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

void PubSubThread::PubSubChannels(const std::string& pattern,
                    std::vector<std::string >* result) {
  if (pattern == "") {
    slash::MutexLock l(&channel_mutex_);
    for (auto& channel : pubsub_channel_) {
      if (channel.second.size() != 0) {
        result->push_back(channel.first);
      }
    }
  } else {
    slash::MutexLock l(&channel_mutex_);
    for (auto& channel : pubsub_channel_) {
      if (slash::stringmatchlen(channel.first.c_str(), channel.first.size(),
                                pattern.c_str(), pattern.size(), 0)) {
        if (channel.second.size() != 0) {
          result->push_back(channel.first);
        }
      }
    }
  }
}

void PubSubThread::PubSubNumSub(const std::vector<std::string> & channels,
                                std::vector<std::pair<std::string, int>>* result) {
  int subscribed;
  slash::MutexLock l(&channel_mutex_);
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
  slash::MutexLock l(&pattern_mutex_);
  for (auto& channel : pubsub_pattern_) {
    subscribed += channel.second.size();
  }
  return subscribed;
}

void *PubSubThread::ThreadMain() {
  int nfds;
  PinkFiredEvent *pfe;
  slash::Status s;
  std::shared_ptr<PinkConn> in_conn = nullptr;
  char triger[1];

  while (!should_stop()) {
    nfds = pink_epoll_->PinkPoll(PINK_CRON_INTERVAL);
    for (int i = 0; i < nfds; i++) {
      pfe = (pink_epoll_->firedevent()) + i;
      if (pfe->fd == pink_epoll_->notify_receive_fd()) {        // New connection comming
        if (pfe->mask & EPOLLIN) {
          read(pink_epoll_->notify_receive_fd(), triger, 1);
          {
            PinkItem ti = pink_epoll_->notify_queue_pop();
            if (ti.notify_type() == kNotiClose) {
            } else if (ti.notify_type() == kNotiEpollout) {
              pink_epoll_->PinkModEvent(ti.fd(), 0, EPOLLOUT);
            } else if (ti.notify_type() == kNotiEpollin) {
              pink_epoll_->PinkModEvent(ti.fd(), 0, EPOLLIN);
            } else if (ti.notify_type() == kNotiEpolloutAndEpollin) {
              pink_epoll_->PinkModEvent(ti.fd(), 0, EPOLLOUT | EPOLLIN);
            } else if (ti.notify_type() == kNotiWait) {
              // do not register events
              pink_epoll_->PinkAddEvent(ti.fd(), 0);
            }
          }
          continue;
        }
      }
      if (pfe->fd == msg_pfd_[0]) {           // Publish message
        if (pfe->mask & EPOLLIN) {
          read(msg_pfd_[0], triger, 1);
          std::string channel, msg;
          int32_t receivers = 0;
          channel = channel_;
          msg = message_;
          channel_.clear();
          message_.clear();

          // Send message to clients
          channel_mutex_.Lock();
          for (auto it = pubsub_channel_.begin(); it != pubsub_channel_.end(); it++) {
            if (channel == it->first) {
              for (size_t i = 0; i < it->second.size(); i++) {
                if (!IsReady(it->second[i]->fd())) {
                  continue;
                }
                std::string resp = ConstructPublishResp(it->first, channel, msg, false);
                it->second[i]->WriteResp(resp);
                WriteStatus write_status = it->second[i]->SendReply();
                if (write_status == kWriteHalf) {
                  pink_epoll_->PinkModEvent(it->second[i]->fd(),
                                            EPOLLIN, EPOLLOUT);
                } else if (write_status == kWriteError) {
                  channel_mutex_.Unlock();

                  MoveConnOut(it->second[i]);

                  channel_mutex_.Lock();
                  CloseFd(it->second[i]);
                } else if (write_status == kWriteAll) {
                  receivers++;
                }
              }
            }
          }
          channel_mutex_.Unlock();

          // Send message to clients
          pattern_mutex_.Lock();
          for (auto it = pubsub_pattern_.begin(); it != pubsub_pattern_.end(); it++) {
            if (slash::stringmatchlen(it->first.c_str(), it->first.size(),
                                      channel.c_str(), channel.size(), 0)) {
              for (size_t i = 0; i < it->second.size(); i++) {
                if (!IsReady(it->second[i]->fd())) {
                  continue;
                }
                std::string resp = ConstructPublishResp(it->first, channel, msg, true);
                it->second[i]->WriteResp(resp);
                WriteStatus write_status = it->second[i]->SendReply();
                if (write_status == kWriteHalf) {
                  pink_epoll_->PinkModEvent(it->second[i]->fd(),
                                            EPOLLIN, EPOLLOUT);
                } else if (write_status == kWriteError) {
                  pattern_mutex_.Unlock();

                  MoveConnOut(it->second[i]);

                  pattern_mutex_.Lock();
                  CloseFd(it->second[i]);
                } else if (write_status == kWriteAll) {
                  receivers++;
                }
              }
            }
          }
          pattern_mutex_.Unlock();

          receiver_mutex_.Lock();
          receivers_ = receivers;
          receiver_rsignal_.Signal();
          receiver_mutex_.Unlock();
        } else {
          continue;
        }
      } else {
        in_conn = NULL;
        bool should_close = false;

        {
          slash::ReadLock l(&rwlock_);
          std::map<int, std::shared_ptr<ConnHandle> >::iterator iter = conns_.find(pfe->fd);
          if (iter == conns_.end()) {
            pink_epoll_->PinkDelEvent(pfe->fd);
            continue;
          }
          in_conn = iter->second->conn;
        }

        // Send reply
        if (pfe->mask & EPOLLOUT && in_conn->is_ready_to_reply()) {
          WriteStatus write_status = in_conn->SendReply();
          if (write_status == kWriteAll) {
            in_conn->set_is_reply(false);
            pink_epoll_->PinkModEvent(pfe->fd, 0, EPOLLIN);  // Remove EPOLLOUT
          } else if (write_status == kWriteHalf) {
            continue;  //  send all write buffer,
                       //  in case of next GetRequest()
                       //  pollute the write buffer
          } else if (write_status == kWriteError) {
            should_close = true;
          }
        }

        // Client request again
        if (!should_close && pfe->mask & EPOLLIN) {
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
              pink_epoll_->PinkModEvent(pfe->fd, EPOLLIN, EPOLLOUT);
            } else if (write_status == kWriteError) {
              should_close = true;
            }
          } else {
            continue;
          }
        }
        // Error
        if ((pfe->mask & EPOLLERR) || (pfe->mask & EPOLLHUP) || should_close) {
          MoveConnOut(in_conn);
          CloseFd(in_conn);
          in_conn = nullptr;
        }
      }
    }
  }
  Cleanup();
  return NULL;
}

void PubSubThread::Cleanup() {
  slash::WriteLock l(&rwlock_);
  for (auto& iter : conns_) {
    CloseFd(iter.second->conn);
  }
  conns_.clear();
}

};  // namespace pink
