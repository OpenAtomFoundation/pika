// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_NEW_MASTER_CONN_H_
#define PIKA_NEW_MASTER_CONN_H_

#include "pink/include/pink_conn.h"
#include "include/pika_command.h"

class PikaBinlogReceiverThread;

class PikaNewMasterConn: public pink::PinkConn {
  public:
    PikaNewMasterConn(int fd, std::string ip_port, void* worker_specific_data);
    virtual ~PikaNewMasterConn();

    virtual pink::ReadStatus GetRequest();
    virtual pink::WriteStatus SendReply();
    virtual void TryResizeBuffer();

    pink::ReadStatus ReadRaw(uint32_t count);
    pink::ReadStatus ReadHeader();
    pink::ReadStatus ReadBody(uint32_t body_lenth);
    void ResetStatus();

    int32_t FindNextSeparators(const std::string& content, int32_t next_parse_pos);
    int32_t GetNextNum(const std::string& content, int32_t next_parse_pos, int32_t pos, long* value);
    pink::ReadStatus ParseRedisRESPArray(const std::string& content, pink::RedisCmdArgsType* argv);

    bool ProcessAuth(const pink::RedisCmdArgsType& argv);
    bool ProcessBinlogData(const pink::RedisCmdArgsType& argv, const BinlogItem& binlog_item);

  private:
    char* rbuf_;
    uint32_t rbuf_len_;
    uint32_t rbuf_size_;
    uint32_t rbuf_cur_pos_;
    bool is_authed_;
    PikaBinlogReceiverThread* binlog_receiver_;
};

#endif

