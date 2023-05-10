// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef MASTER_CONN_H_
#define MASTER_CONN_H_

/*
 * **************Header**************
 * | <Transfer Type> | <Body Lenth> |
 *       2 Bytes         4 Bytes
 */
#define HEADER_LEN 6

#include "net/include/net_conn.h"
#include "pika_command.h"

#include "binlog_transverter.h"

class BinlogReceiverThread;

enum PortTransferOperate { kTypePortAuth = 1, kTypePortBinlog = 2 };

class MasterConn : public net::NetConn {
 public:
  MasterConn(int fd, std::string ip_port, void* worker_specific_data);
  virtual ~MasterConn();

  virtual net::ReadStatus GetRequest();
  virtual net::WriteStatus SendReply();
  virtual void TryResizeBuffer();

  net::ReadStatus ReadRaw(uint32_t count);
  net::ReadStatus ReadHeader();
  net::ReadStatus ReadBody(uint32_t body_lenth);
  void ResetStatus();

  int32_t FindNextSeparators(const std::string& content, int32_t next_parse_pos);
  int32_t GetNextNum(const std::string& content, int32_t next_parse_pos, int32_t pos, long* value);
  net::ReadStatus ParseRedisRESPArray(const std::string& content, net::RedisCmdArgsType* argv);

  bool ProcessAuth(const net::RedisCmdArgsType& argv);
  bool ProcessBinlogData(const net::RedisCmdArgsType& argv, const PortBinlogItem& binlog_item);

 private:
  char* rbuf_;
  uint32_t rbuf_len_;
  uint32_t rbuf_size_;
  uint32_t rbuf_cur_pos_;
  bool is_authed_;
  BinlogReceiverThread* binlog_receiver_;
};

#endif
