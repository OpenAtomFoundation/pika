// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_slave_node.h"

/* SlaveNode */

SlaveNode::SlaveNode(const std::string& ip, int port,
                     const std::string& table_name,
                     uint32_t partition_id, int session_id)
  : RmNode(ip, port, table_name, partition_id, session_id),
  slave_state(kSlaveNotSync),
  b_state(kNotSync), sent_offset(), acked_offset() {
}

SlaveNode::~SlaveNode() {
}

Status SlaveNode::InitBinlogFileReader(const std::shared_ptr<Binlog>& binlog,
                                       const BinlogOffset& offset) {
  binlog_reader = std::make_shared<PikaBinlogReader>();
  int res = binlog_reader->Seek(binlog, offset.filenum, offset.offset);
  if (res) {
    return Status::Corruption(ToString() + "  binlog reader init failed");
  }
  return Status::OK();
}

std::string SlaveNode::ToStringStatus() {
  std::stringstream tmp_stream;
  tmp_stream << "    Slave_state: " << SlaveStateMsg[slave_state] << "\r\n";
  tmp_stream << "    Binlog_sync_state: " << BinlogSyncStateMsg[b_state] << "\r\n";
  tmp_stream << "    Sync_window: " << "\r\n" << sync_win.ToStringStatus();
  tmp_stream << "    Sent_offset: " << sent_offset.ToString() << "\r\n";
  tmp_stream << "    Acked_offset: " << acked_offset.ToString() << "\r\n";
  tmp_stream << "    Binlog_reader activated: " << (binlog_reader != nullptr) << "\r\n";
  return tmp_stream.str();
}


