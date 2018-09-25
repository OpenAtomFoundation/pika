// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <glog/logging.h>

#include "const.h"
#include "master_conn.h"
#include "binlog_receiver_thread.h"

#include "include/pika_command.h"

#include "pika_port.h"

extern PikaPort *g_pika_port;

MasterConn::MasterConn(int fd, std::string ip_port,
                       BinlogReceiverThread* binlog_receiver)
      : RedisConn(fd, ip_port, NULL),
        self_thread_(binlog_receiver) {
}

void MasterConn::RestoreArgs(pink::RedisCmdArgsType& argv) {
  raw_args_.clear();
  size_t num = argv.size();
  if (argv.size() > 4 && *(argv.end() - 4) == kPikaBinlogMagic) {
    num = argv.size()- 4;
  }
  if (argv[1].find(SlotKeyPrefix) != std::string::npos) {
	  return;
  }
  RedisAppendLen(raw_args_, num, "*");
  PikaCmdArgsType::const_iterator it = argv.begin();
  for (size_t idx = 0; idx < num && it != argv.end(); ++ it, ++ idx) {
    RedisAppendLen(raw_args_, (*it).size(), "$");
    RedisAppendContent(raw_args_, *it);
  }
}

int MasterConn::DealMessage(pink::RedisCmdArgsType& argv, std::string* response) {
  //no reply
  //eq set_is_reply(false);

  // if (argv.empty()) {
  if (argv.size() < 5) { // special chars: __PIKA_X#$SKGI\r\n1\r\n[\r\n
    // return -2;
    return 0;
  }

  // if (argv[0] == "auth") {
  //    return 0;
  // }

  RestoreArgs(argv);
  if (raw_args_.size() == 0) {
    return 0;
  }

  // //g_pika_port->logger_->Lock();
  // g_pika_port->logger()->Put(raw_args_);
  // //g_pika_port->logger_->Unlock();

  std::string key(" ");
  if (1 < argv.size()) {
    key = argv[1];
  }
  int ret = g_pika_port->SendRedisCommand(raw_args_, key);
  if (ret != 0) {
    LOG(WARNING) << "send redis command:" << raw_args_ << ", ret:%d" << ret;
  }

  return 0;
}
