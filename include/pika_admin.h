// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_ADMIN_H_
#define PIKA_ADMIN_H_

#include <sstream>

#include "include/pika_command.h"
#include "include/pika_client_conn.h"

/*
 * Admin
 */
class SlaveofCmd : public Cmd {
 public:
  SlaveofCmd()
      : is_noone_(false),
        have_offset_(false),
        filenum_(0),
        pro_offset_(0) {
  }
  virtual void Do();

 private:
  std::string master_ip_;
  int64_t master_port_;
  bool is_noone_;
  bool have_offset_;
  int64_t filenum_;
  int64_t pro_offset_;
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
  virtual void Clear() {
    is_noone_ = false;
    have_offset_ = false;
  }
};

class TrysyncCmd : public Cmd {
 public:
  TrysyncCmd() {}
  virtual void Do();

 private:
  std::string slave_ip_;
  int64_t slave_port_;
  int64_t filenum_;
  int64_t pro_offset_;
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class InternalTrysyncCmd : public Cmd {
 public:
  InternalTrysyncCmd() {}
  virtual void Do();

 private:
  std::string hub_ip_;
  int64_t hub_port_;
  int64_t filenum_;
  int64_t pro_offset_;
  bool send_most_recently_;
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class AuthCmd : public Cmd {
 public:
  AuthCmd() {}
  virtual void Do();

 private:
  std::string pwd_;
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class BgsaveCmd : public Cmd {
 public:
  BgsaveCmd() {}
  virtual void Do();

 private:
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class BgsaveoffCmd : public Cmd {
 public:
  BgsaveoffCmd() {}
  virtual void Do();

 private:
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class CompactCmd : public Cmd {
 public:
  CompactCmd() {}
  virtual void Do();

 private:
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class PurgelogstoCmd : public Cmd {
 public:
  PurgelogstoCmd() : num_(0) {}
  virtual void Do();

 private:
  uint32_t num_;
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class PingCmd : public Cmd {
 public:
  PingCmd() {}
  virtual void Do();

 private:
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class SelectCmd : public Cmd {
 public:
  SelectCmd() {}
  virtual void Do();

 private:
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class FlushallCmd : public Cmd {
 public:
  FlushallCmd() {}
  virtual void Do();

 private:
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class ReadonlyCmd : public Cmd {
 public:
  ReadonlyCmd() : is_open_(false) {}
  virtual void Do();

 private:
  bool is_open_;
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class ClientCmd : public Cmd {
 public:
  ClientCmd() {}
  virtual void Do();
  const static std::string CLIENT_LIST_S;
  const static std::string CLIENT_KILL_S;

 private:
  std::string operation_, ip_port_;
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class InfoCmd : public Cmd {
 public:
  enum InfoSection {
    kInfoErr = 0x0,
    kInfoServer,
    kInfoClients,
    kInfoHub,
    kInfoStats,
    kInfoReplication,
    kInfoKeyspace,
    kInfoBgstats,
    kInfoLog,
    kInfoData,
    kInfoAll,
    kInfoDoubleMaster
  };

  InfoCmd() : rescan_(false), off_(false) {}
  virtual void Do();

 private:
  InfoSection info_section_;
  bool rescan_; //whether to rescan the keyspace
  bool off_;

  const static std::string kAllSection;
  const static std::string kServerSection;
  const static std::string kClientsSection;
  const static std::string kHubSection;
  const static std::string kStatsSection;
  const static std::string kReplicationSection;
  const static std::string kKeyspaceSection;
  const static std::string kLogSection;
  const static std::string kDataSection;
  const static std::string kDoubleMaster;

  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
  virtual void Clear() {
    rescan_ = false;
    off_ = false;
  }

  void InfoServer(std::string &info);
  void InfoClients(std::string &info);
  void InfoHub(std::string &info);
  void InfoStats(std::string &info);
  void InfoReplication(std::string &info);
  void InfoKeyspace(std::string &info);
  void InfoLog(std::string &info);
  void InfoData(std::string &info);
  void InfoDoubleMaster(std::string &info);
};

class ShutdownCmd : public Cmd {
 public:
  ShutdownCmd() {}
  virtual void Do();

 private:
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class ConfigCmd : public Cmd {
 public:
  ConfigCmd() {}
  virtual void Do();

 private:
  std::vector<std::string> config_args_v_;
  virtual void DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
  void ConfigGet(std::string &ret);
  void ConfigSet(std::string &ret);
  void ConfigRewrite(std::string &ret);
  void ConfigResetstat(std::string &ret);
};

class MonitorCmd : public Cmd {
 public:
  MonitorCmd() {}
  virtual void Do();

 private:
  virtual void DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class DbsizeCmd : public Cmd {
 public:
  DbsizeCmd() {}
  virtual void Do();

 private:
  virtual void DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class TimeCmd : public Cmd {
 public:
  TimeCmd() {}
  virtual void Do();

 private:
  virtual void DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class DelbackupCmd : public Cmd {
 public:
  DelbackupCmd() {}
  virtual void Do();

 private:
  virtual void DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

#ifdef TCMALLOC_EXTENSION
class TcmallocCmd : public Cmd {
 public:
  TcmallocCmd() {}
  virtual void Do();

 private:
  int64_t type_;
  int64_t rate_;
  virtual void DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};
#endif
#endif  // PIKA_ADMIN_H_
