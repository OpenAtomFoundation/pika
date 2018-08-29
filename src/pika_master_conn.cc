// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "slash/include/slash_string.h"
#include "slash/include/slash_coding.h"
#include <glog/logging.h>
#include "include/pika_master_conn.h"
#include "include/pika_server.h"
#include "include/pika_conf.h"
#include "include/pika_binlog_receiver_thread.h"

extern PikaServer* g_pika_server;
extern PikaConf* g_pika_conf;

PikaMasterConn::PikaMasterConn(int fd, std::string ip_port,
                               void* worker_specific_data)
      : RedisConn(fd, ip_port, NULL) {
  is_first_send_ = true;
  binlog_receiver_ =
    reinterpret_cast<PikaBinlogReceiverThread*>(worker_specific_data);
}

int PikaMasterConn::DealMessage(
    PikaCmdArgsType& argv, std::string* response) {
  //no reply
  //eq set_is_reply(false);
  if (argv.empty()) {
    return -2;
  }

  g_pika_server->UpdateQueryNumAndExecCountTable(argv[0]);
  // Auth
  if (is_first_send_) {
    if (argv.size() == 2 && argv[0] == "auth") {
      if (argv[1] == std::to_string(g_pika_server->sid())) {
        is_first_send_ = false;
        LOG(INFO) << "BinlogReceiverThread AccessHandle succeeded, My server id: " << g_pika_server->sid() << " Master auth server id: " << argv[1];
        return 0;
      }
      LOG(INFO) << "BinlogReceiverThread AccessHandle failed, My server id: " << g_pika_server->sid() << " Master auth server id: " << argv[1];
    }
    return -2;
  }

  // TODO(shq) maybe monitor do not need these infomation
  BinlogItem binlog_item;
  std::string server_id;
  std::string binlog_info;
  if (!g_pika_server->DoubleMasterMode()) {
    if (argv.size() > 4 &&
        *(argv.end() - 4) == kPikaBinlogMagic) {
      // Record new binlog format
      argv.pop_back();  // send_to_hub flag

      binlog_info = argv.back();  // binlog_info
      argv.pop_back();
      uint32_t exec_time = 0;
      uint32_t filenum = 0;
      uint64_t offset = 0;
      slash::GetFixed32(&binlog_info, &exec_time);
      slash::GetFixed32(&binlog_info, &filenum);
      slash::GetFixed64(&binlog_info, &offset);
      binlog_item.set_exec_time(exec_time);
      binlog_item.set_filenum(filenum);
      binlog_item.set_offset(offset);

      server_id = argv.back();  // server_id
      argv.pop_back();
      binlog_item.set_server_id(std::atoi(server_id.c_str()));

      argv.pop_back();  //  kPikaBinlogMagic
    }
  }

  // Monitor related
  std::string monitor_message;
  if (g_pika_server->HasMonitorClients()) {
    std::string monitor_message = std::to_string(1.0*slash::NowMicros()/1000000)
      + " [" + this->ip_port() + "]";
    for (PikaCmdArgsType::iterator iter = argv.begin(); iter != argv.end(); iter++) {
      monitor_message += " " + slash::ToRead(*iter);
    }
    g_pika_server->AddMonitorMessage(monitor_message);
  }

  bool is_readonly = g_pika_conf->readonly();

  // Here, the binlog dispatch thread, instead of the binlog bgthread takes on the task to write binlog
  // Only when the server is readonly
  uint64_t serial = binlog_receiver_->GetnPlusSerial();
  if (is_readonly) {
    if (!g_pika_server->WaitTillBinlogBGSerial(serial)) {
      return -2;
    }
    std::string opt = slash::StringToLower(argv[0]);
    Cmd* c_ptr = binlog_receiver_->GetCmd(opt);

    g_pika_server->logger_->Lock();
    g_pika_server->logger_->Put(c_ptr->ToBinlog(argv,
                                                binlog_item.exec_time(),
                                                std::to_string(binlog_item.server_id()),
                                                binlog_item.logic_id(),
                                                binlog_item.filenum(),
                                                binlog_item.offset()));
    g_pika_server->logger_->Unlock();
    g_pika_server->SignalNextBinlogBGSerial();
  }

  PikaCmdArgsType *v = new PikaCmdArgsType(argv);
  BinlogItem *b = new BinlogItem(binlog_item);
  std::string dispatch_key = argv.size() >= 2 ? argv[1] : argv[0];
  g_pika_server->DispatchBinlogBG(dispatch_key, v, b, serial, is_readonly);

  return 0;
}
