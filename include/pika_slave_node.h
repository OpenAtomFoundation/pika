// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_SLAVE_NODE_H_
#define PIKA_SLAVE_NODE_H_

#include <deque>
#include <memory>

#include "include/pika_define.h"
#include "include/pika_binlog_reader.h"

struct SyncWinItem {
  LogOffset offset_;
  std::size_t binlog_size_;
  bool acked_;
  bool operator==(const SyncWinItem& other) const {
    return offset_.b_offset.filenum == other.offset_.b_offset.filenum
      && offset_.b_offset.offset == other.offset_.b_offset.offset;
  }
  explicit SyncWinItem(const LogOffset& offset, std::size_t binlog_size = 0)
    : offset_(offset), binlog_size_(binlog_size), acked_(false) {
  }
  std::string ToString() const {
    return offset_.ToString() + " binglog size: " + std::to_string(binlog_size_) +
      " acked: " + std::to_string(acked_);
  }
};

class SyncWindow {
 public:
  SyncWindow() :total_size_(0) {
  }
  void Push(const SyncWinItem& item);
  bool Update(const SyncWinItem& start_item, const SyncWinItem& end_item, LogOffset* acked_offset);
  int Remaining();
  std::string ToStringStatus() const {
    if (win_.empty()) {
      return "      Size: " + std::to_string(win_.size()) + "\r\n";
    } else {
      std::string res;
      res += "      Size: " + std::to_string(win_.size()) + "\r\n";
      res += ("      Begin_item: " + win_.begin()->ToString() + "\r\n");
      res += ("      End_item: " + win_.rbegin()->ToString() + "\r\n");
      return res;
    }
  }
  std::size_t GetTotalBinlogSize() {
    return total_size_;
  }
  void Reset() {
    win_.clear();
    total_size_ = 0;
  }

 private:
  // TODO(whoiami) ring buffer maybe
  std::deque<SyncWinItem> win_;
  std::size_t total_size_;
};

// role master use
class SlaveNode : public RmNode {
 public:
  SlaveNode(const std::string& ip, int port, const std::string& table_name, uint32_t partition_id, int session_id);
  ~SlaveNode();
  void Lock() {
    slave_mu.Lock();
  }
  void Unlock() {
    slave_mu.Unlock();
  }
  SlaveState slave_state;

  BinlogSyncState b_state;
  SyncWindow sync_win;
  LogOffset sent_offset;
  LogOffset acked_offset;

  std::string ToStringStatus();

  std::shared_ptr<PikaBinlogReader> binlog_reader;
  Status InitBinlogFileReader(const std::shared_ptr<Binlog>& binlog, const BinlogOffset& offset);
  Status Update(const LogOffset& start, const LogOffset& end, LogOffset* updated_offset);

  pstd::Mutex slave_mu;
};

#endif  // PIKA_SLAVE_NODE_H
