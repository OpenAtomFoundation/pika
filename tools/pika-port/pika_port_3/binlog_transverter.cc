// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "binlog_transverter.h"
#include <glog/logging.h>
#include "storage/storage_time.h"

uint32_t PortBinlogItem::exec_time() const { return exec_time_; }

uint32_t PortBinlogItem::server_id() const { return server_id_; }

uint64_t PortBinlogItem::logic_id() const { return logic_id_; }

uint32_t PortBinlogItem::filenum() const { return filenum_; }

uint64_t PortBinlogItem::offset() const { return offset_; }

const std::string& PortBinlogItem::content() const { return content_; }

void PortBinlogItem::set_exec_time(uint32_t exec_time) { exec_time_ = exec_time; }

void PortBinlogItem::set_server_id(uint32_t server_id) { server_id_ = server_id; }

void PortBinlogItem::set_logic_id(uint64_t logic_id) { logic_id_ = logic_id; }

void PortBinlogItem::set_filenum(uint32_t filenum) { filenum_ = filenum; }

void PortBinlogItem::set_offset(uint64_t offset) { offset_ = offset; }

std::string PortBinlogItem::ToString() const {
  std::string str;
  str.append("exec_time: " + std::to_string(exec_time_));
  str.append(",server_id: " + std::to_string(server_id_));
  str.append(",logic_id: " + std::to_string(logic_id_));
  str.append(",filenum: " + std::to_string(filenum_));
  str.append(",offset: " + std::to_string(offset_));
  str.append("\ncontent: ");
  for (char idx : content_) {
    if (idx == '\n') {
      str.append("\\n");
    } else if (idx == '\r') {
      str.append("\\r");
    } else {
      str.append(1, idx);
    }
  }
  str.append("\n");
  return str;
}

std::string PortBinlogTransverter::PortBinlogEncode(PortBinlogType type, uint32_t exec_time, uint32_t server_id,
                                                    uint64_t logic_id, uint32_t filenum, uint64_t offset,
                                                    const std::string& content,
                                                    const std::vector<std::string>& extends) {
  std::string binlog;
  pstd::PutFixed16(&binlog, type);
  pstd::PutFixed32(&binlog, exec_time);
  pstd::PutFixed32(&binlog, server_id);
  pstd::PutFixed64(&binlog, logic_id);
  pstd::PutFixed32(&binlog, filenum);
  pstd::PutFixed64(&binlog, offset);
  uint32_t content_length = content.size();
  pstd::PutFixed32(&binlog, content_length);
  binlog.append(content);
  return binlog;
}

bool PortBinlogTransverter::PortBinlogDecode(PortBinlogType type, const std::string& binlog,
                                             PortBinlogItem* binlog_item) {
  uint16_t binlog_type = 0;
  uint32_t content_length = 0;
  std::string binlog_str = binlog;
  pstd::GetFixed16(&binlog_str, &binlog_type);
  if (binlog_type != type) {
    LOG(WARNING) << "PortBinlog Item type error, expect type: " << static_cast<uint16_t>(type)
                 << " actualy type: " << binlog_type;
    return false;
  }
  storage::g_storage_logictime->UpdateLogicTime(binlog_item->exec_time());
  pstd::GetFixed32(&binlog_str, &binlog_item->exec_time_);
  pstd::GetFixed32(&binlog_str, &binlog_item->server_id_);
  pstd::GetFixed64(&binlog_str, &binlog_item->logic_id_);
  pstd::GetFixed32(&binlog_str, &binlog_item->filenum_);
  pstd::GetFixed64(&binlog_str, &binlog_item->offset_);
  pstd::GetFixed32(&binlog_str, &content_length);
  if (binlog_str.size() >= content_length) {
    binlog_item->content_.assign(binlog_str.data(), content_length);
  } else {
    LOG(WARNING) << "PortBinlog Item get content error, expect length: " << content_length
                 << ", left length: " << binlog_str.size();
    return false;
  }
  binlog_str.erase(0, content_length);
  return true;
}
