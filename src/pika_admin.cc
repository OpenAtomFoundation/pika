#include "slash_string.h"
#include "pika_admin.h"
#include "pika_server.h"

extern PikaServer *g_pika_server;

void SlaveofCmd::Initial(PikaCmdArgsType &argv) {
  if (!GetCmdInfo(kCmdNameSlaveof)->CheckArg(argv.size())) {
    res_.SetErr("wrong number of arguments for " + GetCmdInfo(kCmdNameSet)->name() + " command");
    return;
  }
  PikaCmdArgsType::iterator it = argv.begin() + 1; //Remember the first args is the opt name
  master_ip_ = *it++;
  std::string str_master_port = *it++;
  if (!slash::string2l(str_master_port.data(), str_master_port.size(), &master_port_) && master_port_ <= 0) {
    res_.SetErr("value is not an integer or out of range");
    return;
  }

  int cur_size = argv.end() - it;
  if (cur_size == 0) {

  } else if (cur_size == 2) {
    have_offset_ = true;
    std::string str_filenum = *it++;
    if (!slash::string2l(str_filenum.data(), str_filenum.size(), &filenum_) && filenum_ < 0) {
      res_.SetErr("value is not an integer or out of range");
      return;
    }
    std::string str_pro_offset = *it++;
    if (!slash::string2l(str_pro_offset.data(), str_pro_offset.size(), &pro_offset_) && pro_offset_ < 0) {
      res_.SetErr("value is not an integer or out of range");
      return;
    }
  } else {
    res_.SetErr("wrong number of arguments for " + GetCmdInfo(kCmdNameSet)->name() + " command");
  }
}

void SlaveofCmd::Do(PikaCmdArgsType &argv) {
  Initial(argv);
  if (!res_.ok()) {
    return;
  }
  if (have_offset_) {
    g_pika_server->logger_->SetProducerStatus(filenum_, pro_offset_);
  }
  bool sm_ret = g_pika_server->SetMaster(master_ip_, master_port_);
  if (sm_ret) {
    res_.SetContent("+OK");
  } else {
    res_.SetErr("Server is not in correct state for slaveof");
  }
}

