// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "net/include/pb_conn.h"

#include <arpa/inet.h>
#include <string>

#include <glog/logging.h>

#include "net/include/net_define.h"
#include "net/include/net_stats.h"
#include "pstd/include/xdebug.h"

extern std::unique_ptr<net::NetworkStatistic> g_network_statistic;

namespace net {

PbConn::PbConn(const int fd, const std::string& ip_port, Thread* thread, NetMultiplexer* mpx)
    : NetConn(fd, ip_port, thread, mpx),
      
      write_buf_(0)
      {
  rbuf_ = reinterpret_cast<char*>(malloc(sizeof(char) * PB_IOBUF_LEN));
  rbuf_len_ = PB_IOBUF_LEN;
}

PbConn::~PbConn() { free(rbuf_); }

// Msg is [ length(COMMAND_HEADER_LENGTH) | body(length bytes) ]
//   step 1. kHeader, we read COMMAND_HEADER_LENGTH bytes;
//   step 2. kPacket, we read header_len bytes;
ReadStatus PbConn::GetRequest() {
  while (true) {
    switch (connStatus_) {
      case kHeader: {
        ssize_t nread = read(fd(), rbuf_ + cur_pos_, COMMAND_HEADER_LENGTH - cur_pos_);
        if (nread == -1) {
          if (errno == EAGAIN) {
            return kReadHalf;
          } else {
            return kReadError;
          }
        } else if (nread == 0) {
          return kReadClose;
        } else {
          g_network_statistic->IncrReplInputBytes(nread);
          cur_pos_ += nread;
          if (cur_pos_ == COMMAND_HEADER_LENGTH) {
            uint32_t integer = 0;
            memcpy(reinterpret_cast<char*>(&integer), rbuf_, sizeof(uint32_t));
            header_len_ = ntohl(integer);
            remain_packet_len_ = static_cast<int32_t>(header_len_);
            connStatus_ = kPacket;
            continue;
          }
          return kReadHalf;
        }
      }
      case kPacket: {
        if (header_len_ > rbuf_len_ - COMMAND_HEADER_LENGTH) {
          uint32_t new_size = header_len_ + COMMAND_HEADER_LENGTH;
          if (new_size < kProtoMaxMessage) {
            rbuf_ = reinterpret_cast<char*>(realloc(rbuf_, sizeof(char) * new_size));
            if (!rbuf_) {
              return kFullError;
            }
            rbuf_len_ = new_size;
            LOG(INFO) << "Thread_id " << pthread_self() << " Expand rbuf to " << new_size << ", cur_pos_ " << cur_pos_;
          } else {
            return kFullError;
          }
        }
        // read msg body
        ssize_t nread = read(fd(), rbuf_ + cur_pos_, remain_packet_len_);
        if (nread == -1) {
          if (errno == EAGAIN) {
            return kReadHalf;
          } else {
            return kReadError;
          }
        } else if (nread == 0) {
          return kReadClose;
        }
        g_network_statistic->IncrReplInputBytes(nread);
        cur_pos_ += static_cast<uint32_t>(nread);
        remain_packet_len_ -= static_cast<int32_t>(nread);
        if (remain_packet_len_ == 0) {
          connStatus_ = kComplete;
          continue;
        }
        return kReadHalf;
      }
      case kComplete: {
        if (DealMessage() != 0) {
          return kDealError;
        }
        connStatus_ = kHeader;
        cur_pos_ = 0;
        return kReadAll;
      }
      // Add this switch case just for delete compile warning
      case kBuildObuf:
        break;

      case kWriteObuf:
        break;
    }
  }

  return kReadHalf;
}

WriteStatus PbConn::SendReply() {
  ssize_t nwritten = 0;
  size_t item_len;
  std::lock_guard l(resp_mu_);
  while (!write_buf_.queue_.empty()) {
    std::string item = write_buf_.queue_.front();
    item_len = item.size();
    while (item_len - write_buf_.item_pos_ > 0) {
      nwritten = write(fd(), item.data() + write_buf_.item_pos_, item_len - write_buf_.item_pos_);
      if (nwritten <= 0) {
        break;
      }
      g_network_statistic->IncrReplOutputBytes(nwritten);
      write_buf_.item_pos_ += nwritten;
      if (write_buf_.item_pos_ == item_len) {
        write_buf_.queue_.pop();
        write_buf_.item_pos_ = 0;
        item_len = 0;
      }
    }
    if (nwritten == -1) {
      if (errno == EAGAIN) {
        return kWriteHalf;
      } else {
        // Here we should close the connection
        return kWriteError;
      }
    }
    if (item_len - write_buf_.item_pos_ != 0) {
      return kWriteHalf;
    }
  }
  return kWriteAll;
}

void PbConn::set_is_reply(const bool is_reply) {
  std::lock_guard l(is_reply_mu_);
  if (is_reply) {
    is_reply_++;
  } else {
    is_reply_--;
  }
  if (is_reply_ < 0) {
    is_reply_ = 0;
  }
}

bool PbConn::is_reply() {
  std::lock_guard l(is_reply_mu_);
  return is_reply_ > 0;
}

int PbConn::WriteResp(const std::string& resp) {
  std::string tag;
  BuildInternalTag(resp, &tag);
  std::lock_guard l(resp_mu_);
  write_buf_.queue_.push(tag);
  write_buf_.queue_.push(resp);
  set_is_reply(true);
  return 0;
}

void PbConn::BuildInternalTag(const std::string& resp, std::string* tag) {
  uint32_t resp_size = resp.size();
  resp_size = htonl(resp_size);
  *tag = std::string(reinterpret_cast<char*>(&resp_size), 4);
}

void PbConn::TryResizeBuffer() {
  struct timeval now;
  gettimeofday(&now, nullptr);
  time_t idletime = now.tv_sec - last_interaction().tv_sec;
  if (rbuf_len_ > PB_IOBUF_LEN && ((rbuf_len_ / (cur_pos_ + 1)) > 2 || idletime > 2)) {
    uint32_t new_size = ((cur_pos_ + PB_IOBUF_LEN) / PB_IOBUF_LEN) * PB_IOBUF_LEN;
    if (new_size < rbuf_len_) {
      rbuf_ = static_cast<char*>(realloc(rbuf_, new_size));
      rbuf_len_ = new_size;
      LOG(INFO) << "Thread_id " << pthread_self() << "Shrink rbuf to " << rbuf_len_ << ", cur_pos_: " << cur_pos_;
    }
  }
}

void PbConn::NotifyWrite() {
  net::NetItem ti(fd(), ip_port(), net::kNotiWrite);
  net_multiplexer()->Register(ti, true);
}

void PbConn::NotifyClose() {
  net::NetItem ti(fd(), ip_port(), net::kNotiClose);
  net_multiplexer()->Register(ti, true);
}

}  // namespace net
