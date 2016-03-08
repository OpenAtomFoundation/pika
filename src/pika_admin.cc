#include "slash_string.h"
#include "pika_admin.h"
#include "pika_server.h"

extern PikaServer *g_pika_server;

void SlaveofCmd::Initial(PikaCmdArgsType &argv) {
  if (!GetCmdInfo(kCmdNameSlaveof)->CheckArg(argv.size())) {
    res_.SetErr("wrong number of arguments for " + GetCmdInfo(kCmdNameSlaveof)->name() + " command");
    return;
  }
  PikaCmdArgsType::iterator it = argv.begin() + 1; //Remember the first args is the opt name

  master_ip_ = slash::StringToLower(*it++);

  is_noone_ = false;
  if (master_ip_ == "no" && slash::StringToLower(*it++) == "one") {
    if (argv.end() - it == 0) {
      is_noone_ = true;
    } else {
      res_.SetErr("wrong number of arguments for " + GetCmdInfo(kCmdNameSlaveof)->name() + " command");
    }
    return;
  }

  std::string str_master_port = *it++;
  if (!slash::string2l(str_master_port.data(), str_master_port.size(), &master_port_) && master_port_ <= 0) {
    res_.SetErr("value is not an integer or out of range");
    return;
  }

  if ((master_ip_ == "127.0.0.1" || master_ip_ == g_pika_server->host()) && master_port_ == g_pika_server->port()) {
    res_.SetErr("you fucked up");
    return;
  }

  have_offset_ = false;
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
  if (is_noone_) {
    g_pika_server->RemoveMaster();
    res_.SetContent("+OK");
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

void TrysyncCmd::Initial(PikaCmdArgsType &argv) {
  if (!GetCmdInfo(kCmdNameTrysync)->CheckArg(argv.size())) {
    res_.SetErr("wrong number of arguments for " + GetCmdInfo(kCmdNameTrysync)->name() + " command");
    return;
  }
  PikaCmdArgsType::iterator it = argv.begin() + 1; //Remember the first args is the opt name
  slave_ip_ = *it++;

  std::string str_slave_port = *it++;
  if (!slash::string2l(str_slave_port.data(), str_slave_port.size(), &slave_port_) && slave_port_ <= 0) {
    res_.SetErr("value is not an integer or out of range");
    return;
  }

  std::string str_filenum = *it++;
  if (!slash::string2l(str_filenum.data(), str_filenum.size(), &filenum_) && filenum_ <= 0) {
    res_.SetErr("value is not an integer or out of range");
    return;
  }

  std::string str_pro_offset = *it++;
  if (!slash::string2l(str_pro_offset.data(), str_pro_offset.size(), &pro_offset_) && pro_offset_ <= 0) {
    res_.SetErr("value is not an integer or out of range");
    return;
  }

}

void TrysyncCmd::Do(PikaCmdArgsType &argv) {
  Initial(argv);
  if (!res_.ok()) {
    return;
  }
  std::string ip_port = slave_ip_;
  char buf[10];
  slash::ll2string(buf, sizeof(buf), slave_port_);
  ip_port.append(":");
  ip_port.append(buf);
  DLOG(INFO) << "Trysync, Slave ip_port: " << ip_port << " filenum: " << filenum_ << " pro_offset: " << pro_offset_;
  if (!g_pika_server->FindSlave(ip_port)) {
    SlaveItem s;
    s.sid = g_pika_server->GenSid();
    s.ip_port = ip_port;
    s.port = slave_port_;
    s.hb_fd = -1;
    s.stage = SLAVE_ITEM_STAGE_ONE;
    gettimeofday(&s.create_time, NULL);
    s.sender = NULL;
    
    DLOG(INFO) << "Trysync, dont FindSlave, so AddBinlogSender";
    Status status = g_pika_server->AddBinlogSender(s, filenum_, pro_offset_);
    if (status.ok()) {
      char tmp[128];
      strcpy(tmp, "+");
      slash::ll2string(buf, sizeof(buf), s.sid);
      strcat(tmp, buf);
      DLOG(INFO) << "Send Sid to Slave: " << tmp;
      g_pika_server->BecomeMaster();
      res_.SetContent(tmp);
    } else {
      res_.SetErr("Error in AddBinlogSender");
    }
  } else {
    res_.SetErr("Already Exist");
  }
}

