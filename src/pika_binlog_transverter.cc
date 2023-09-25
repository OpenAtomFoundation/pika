// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_binlog_transverter.h"

#include <glog/logging.h>
#include <cassert>
#include <sstream>

#include "pstd/include/pstd_coding.h"

#include "include/pika_command.h"
#include "storage/storage.h"


uint32_t BinlogItem::exec_time() const { return exec_time_; }

uint32_t BinlogItem::term_id() const { return term_id_; }

uint64_t BinlogItem::logic_id() const { return logic_id_; }

uint32_t BinlogItem::filenum() const { return filenum_; }

uint64_t BinlogItem::offset() const { return offset_; }

std::string BinlogItem::content() const { return content_; }

void BinlogItem::set_exec_time(uint32_t exec_time) { exec_time_ = exec_time; }

void BinlogItem::set_term_id(uint32_t term_id) { term_id_ = term_id; }

void BinlogItem::set_logic_id(uint64_t logic_id) { logic_id_ = logic_id; }

void BinlogItem::set_filenum(uint32_t filenum) { filenum_ = filenum; }

void BinlogItem::set_offset(uint64_t offset) { offset_ = offset; }

std::string BinlogItem::ToString() const {
  std::string str;
  str.append("exec_time: " + std::to_string(exec_time_));
  str.append(",term_id: " + std::to_string(term_id_));
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

std::string PikaBinlogTransverter::BinlogEncode(BinlogType type, uint32_t exec_time, uint32_t term_id,
                                                uint64_t logic_id, uint32_t filenum, uint64_t offset,
                                                const std::string& content, const std::vector<std::string>& extends) {
  std::string binlog;
  pstd::PutFixed16(&binlog, type);
  pstd::PutFixed32(&binlog, exec_time);
  pstd::PutFixed32(&binlog, term_id);
  pstd::PutFixed64(&binlog, logic_id);
  pstd::PutFixed32(&binlog, filenum);
  pstd::PutFixed64(&binlog, offset);
  uint32_t content_length = content.size();
  pstd::PutFixed32(&binlog, content_length);
  binlog.append(content);
  return binlog;
}

bool PikaBinlogTransverter::BinlogDecode(BinlogType type, const std::string& binlog, BinlogItem* binlog_item) {
  uint16_t binlog_type = 0;
  uint32_t content_length = 0;
  pstd::Slice binlog_str = binlog;
  pstd::GetFixed16(&binlog_str, &binlog_type);
  if (binlog_type != type) {
    LOG(ERROR) << "Binlog Item type error, expect type:" << type << " actualy type: " << binlog_type;
    return false;
  }
  pstd::GetFixed32(&binlog_str, &binlog_item->exec_time_);
  pstd::GetFixed32(&binlog_str, &binlog_item->term_id_);
  pstd::GetFixed64(&binlog_str, &binlog_item->logic_id_);
  pstd::GetFixed32(&binlog_str, &binlog_item->filenum_);
  pstd::GetFixed64(&binlog_str, &binlog_item->offset_);
  pstd::GetFixed32(&binlog_str, &content_length);
  if (binlog_str.size() == content_length) {
    binlog_item->content_.assign(binlog_str.data(), content_length);
  } else {
    LOG(ERROR) << "Binlog Item get content error, expect length:" << content_length
               << " left length:" << binlog_str.size();
    return false;
  }
  return true;
}

std::string PikaBinlogTransverter::RaftlogEncode(BinlogType type, uint32_t exec_time,
                                                const std::string& content, const std::vector<std::string>& extends) {
  std::string raftlog;
  pstd::PutFixed16(&raftlog, type);
  pstd::PutFixed32(&raftlog, exec_time);
  uint32_t content_length = content.size();
  pstd::PutFixed32(&raftlog, content_length);
  raftlog.append(content);
  return raftlog;
}

// use BinlogItem to contain raft log with {term_id, logic_id, filenum, offset} keep 0
bool PikaBinlogTransverter::RaftlogDecode(BinlogType type, const std::string& raftlog, BinlogItem* raftlog_item) {
  uint16_t raftlog_type = 0;
  uint32_t content_length = 0;
  pstd::Slice raftlog_str = raftlog;
  pstd::GetFixed16(&raftlog_str, &raftlog_type);
  if (raftlog_type != type) {
    LOG(ERROR) << "Raftlog Item type error, expect type:" << type << " actualy type: " << raftlog_type;
    return false;
  }
  pstd::GetFixed32(&raftlog_str, &raftlog_item->exec_time_);
  pstd::GetFixed32(&raftlog_str, &content_length);
  if (raftlog_str.size() == content_length) {
    raftlog_item->content_.assign(raftlog_str.data(), content_length);
  } else {
    LOG(ERROR) << "Rasftlog Item get content error, expect length:" << content_length
               << " left length:" << raftlog_str.size();
    return false;
  }
  return true;
}

/*
******************* Type First Binlog Item Format ******************
 * +-----------------------------------------------------------------+
 * | Type (2 bytes) | Create Time (4 bytes) | Term Id (4 bytes)      |
 * |-----------------------------------------------------------------|
 * | Logic Id (8 bytes) | File Num (4 bytes) | Offset (8 bytes)      |
 * |-----------------------------------------------------------------|
 * | Content Length (4 bytes) | Content (content length bytes)       |
 * +-----------------------------------------------------------------+
 * |------------------------ 34 Bytes -------------------------------|
 *
 * content: *2\r\n$7\r\npadding\r\n$00001\r\n***\r\n
 *          length of *** -> total_len - PADDING_BINLOG_PROTOCOL_SIZE - SPACE_STROE_PARAMETER_LENGTH;
 *
 * We allocate five bytes to store the length of the parameter
 */
std::string PikaBinlogTransverter::ConstructPaddingBinlog(BinlogType type, uint32_t size) {
  assert(size <= kBlockSize - kHeaderSize);
  assert(BINLOG_ITEM_HEADER_SIZE + PADDING_BINLOG_PROTOCOL_SIZE + SPACE_STROE_PARAMETER_LENGTH <= size);

  std::string binlog;
  pstd::PutFixed16(&binlog, type);
  pstd::PutFixed32(&binlog, 0);
  pstd::PutFixed32(&binlog, 0);
  pstd::PutFixed64(&binlog, 0);
  pstd::PutFixed32(&binlog, 0);
  pstd::PutFixed64(&binlog, 0);
  auto content_len = static_cast<int32_t>(size - BINLOG_ITEM_HEADER_SIZE);
  int32_t parameter_len = content_len - PADDING_BINLOG_PROTOCOL_SIZE - SPACE_STROE_PARAMETER_LENGTH;
  if (parameter_len < 0) {
    return {};
  }

  std::string content;
  RedisAppendLen(content, 2, "*");
  RedisAppendLen(content, 7, "$");
  RedisAppendContent(content, "padding");

  std::string parameter_len_str;
  std::ostringstream os;
  os << parameter_len;
  std::istringstream is(os.str());
  is >> parameter_len_str;
  if (parameter_len_str.size() > SPACE_STROE_PARAMETER_LENGTH) {
    return {};
  }

  content.append("$");
  content.append(SPACE_STROE_PARAMETER_LENGTH - parameter_len_str.size(), '0');
  content.append(parameter_len_str);
  content.append(kNewLine);
  RedisAppendContent(content, std::string(parameter_len, '*'));

  pstd::PutFixed32(&binlog, content_len);
  binlog.append(content);
  return binlog;
}

bool PikaBinlogTransverter::BinlogItemWithoutContentDecode(BinlogType type, const std::string& binlog,
                                                           BinlogItem* binlog_item) {
  uint16_t binlog_type = 0;
  pstd::Slice binlog_str = binlog;
  pstd::GetFixed16(&binlog_str, &binlog_type);
  if (binlog_type != type) {
    LOG(ERROR) << "Binlog Item type error, expect type:" << type << " actualy type: " << binlog_type;
    return false;
  }
  pstd::GetFixed32(&binlog_str, &binlog_item->exec_time_);
  pstd::GetFixed32(&binlog_str, &binlog_item->term_id_);
  pstd::GetFixed64(&binlog_str, &binlog_item->logic_id_);
  pstd::GetFixed32(&binlog_str, &binlog_item->filenum_);
  pstd::GetFixed64(&binlog_str, &binlog_item->offset_);
  return true;
}
