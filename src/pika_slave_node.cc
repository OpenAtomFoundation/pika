// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_slave_node.h"
#include "pstd/include/pika_conf.h"

using pstd::Status;

extern std::unique_ptr<PikaConf> g_pika_conf;

/* SyncWindow */

void SyncWindow::Push(const SyncWinItem& item) {
  win_.push_back(item);
  total_size_ += item.binlog_size_;
}

bool SyncWindow::Update(const SyncWinItem& start_item, const SyncWinItem& end_item, LogOffset* acked_offset) {
  size_t start_pos = win_.size();
  size_t end_pos = win_.size();
  for (size_t i = 0; i < win_.size(); ++i) {
    if (win_[i] == start_item) {
      start_pos = i;
    }
    if (win_[i] == end_item) {
      end_pos = i;
      break;
    }
  }
  if (start_pos == win_.size() || end_pos == win_.size()) {
    LOG(WARNING) << "Ack offset Start: " << start_item.ToString() << "End: " << end_item.ToString()
                 << " not found in binlog controller window." << std::endl
                 << "window status " << std::endl
                 << ToStringStatus();
    return false;
  }
  for (size_t i = start_pos; i <= end_pos; ++i) {
    win_[i].acked_ = true;
    total_size_ -= win_[i].binlog_size_;
  }
  while (!win_.empty()) {
    if (win_[0].acked_) {
      *acked_offset = win_[0].offset_;
      win_.pop_front();
    } else {
      break;
    }
  }
  return true;
}

int SyncWindow::Remaining() {
  std::size_t remaining_size = g_pika_conf->sync_window_size() - win_.size();
  return static_cast<int>(remaining_size > 0 ? remaining_size : 0);
}

/* SlaveNode */

SlaveNode::SlaveNode(const std::string& ip, int port, const std::string& db_name, int session_id)
    : RmNode(ip, port, db_name, session_id)
      
      {}

SlaveNode::~SlaveNode() = default;

Status SlaveNode::InitBinlogFileReader(const std::shared_ptr<Binlog>& binlog, const BinlogOffset& offset) {
  binlog_reader = std::make_shared<PikaBinlogReader>();
  int res = binlog_reader->Seek(binlog, offset.filenum, offset.offset);
  if (res != 0) {
    return Status::Corruption(ToString() + "  binlog reader init failed");
  }
  return Status::OK();
}

std::string SlaveNode::ToStringStatus() {
  std::stringstream tmp_stream;
  tmp_stream << "    Slave_state: " << SlaveStateMsg[slave_state] << "\r\n";
  tmp_stream << "    Binlog_sync_state: " << BinlogSyncStateMsg[b_state] << "\r\n";
  tmp_stream << "    Sync_window: "
             << "\r\n"
             << sync_win.ToStringStatus();
  tmp_stream << "    Sent_offset: " << sent_offset.ToString() << "\r\n";
  tmp_stream << "    Acked_offset: " << acked_offset.ToString() << "\r\n";
  tmp_stream << "    Binlog_reader activated: " << (binlog_reader != nullptr) << "\r\n";
  return tmp_stream.str();
}

Status SlaveNode::Update(const LogOffset& start, const LogOffset& end, LogOffset* updated_offset) {
  if (slave_state != kSlaveBinlogSync) {
    return Status::Corruption(ToString() + "state not BinlogSync");
  }
  *updated_offset = LogOffset();
  bool res = sync_win.Update(SyncWinItem(start), SyncWinItem(end), updated_offset);
  if (!res) {
    return Status::Corruption("UpdateAckedInfo failed");
  }
  if (*updated_offset == LogOffset()) {
    // nothing to update return current acked_offset
    *updated_offset = acked_offset;
    return Status::OK();
  }
  // update acked_offset
  acked_offset = *updated_offset;
  return Status::OK();
}
