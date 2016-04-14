#ifndef __PIKA_ADMIN_H__
#define __PIKA_ADMIN_H__
#include "pika_command.h"

/*
 * Admin
 */
class SlaveofCmd : public Cmd {
public:
  SlaveofCmd() : is_noone_(false), have_offset_(false),
  filenum_(0), pro_offset_(0) {
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
  TrysyncCmd() {
  }
  virtual void Do();
private:
  std::string slave_ip_;
  int64_t slave_port_;
  int64_t filenum_;
  int64_t pro_offset_;
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class AuthCmd : public Cmd {
public:
  AuthCmd() {
  }
  virtual void Do();
private:
  std::string pwd_;
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class BgsaveCmd : public Cmd {
public:
  BgsaveCmd() {
  }
  virtual void Do();
private:
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class BgsaveoffCmd : public Cmd {
public:
  BgsaveoffCmd() {
  }
  virtual void Do();
private:
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class CompactCmd : public Cmd {
public:
  CompactCmd() {
  }
  virtual void Do();
private:
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class PurgelogstoCmd : public Cmd {
public:
  PurgelogstoCmd() : num_(0){
  }
  virtual void Do();
private:
  uint32_t num_;
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class PingCmd : public Cmd {
public:
  PingCmd() {
  }
  virtual void Do();
private:
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class SelectCmd : public Cmd {
public:
  SelectCmd() {
  }
  virtual void Do();
private:
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class FlushallCmd : public Cmd {
public:
  FlushallCmd() {
  }
  virtual void Do();
private:
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class ReadonlyCmd : public Cmd {
public:
  ReadonlyCmd() : is_open_(false) {
  }
  virtual void Do();
private:
  bool is_open_;
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class ClientCmd : public Cmd {
public:
  ClientCmd() {
  }
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
    kInfoStats,
    kInfoReplication,
    kInfoKeyspace,
    kInfoAll
  };

  InfoCmd() : rescan_(false) {
  }
  virtual void Do();
private:
  InfoSection info_section_;
  bool rescan_; //whether to rescan the keyspace

  const static std::string kAllSection;
  const static std::string kServerSection;
  const static std::string kClientsSection;
  const static std::string kStatsSection;
  const static std::string kReplicationSection;
  const static std::string kKeyspaceSection;

  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
  virtual void Clear() {
    rescan_ = false;
  }
   
  void InfoServer(std::string &info);
  void InfoClients(std::string &info);
  void InfoStats(std::string &info);
  void InfoReplication(std::string &info);
  void InfoKeyspace(std::string &info);
};

class ShutdownCmd : public Cmd {
public:
  ShutdownCmd() {
  }
  virtual void Do();
private:
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class ConfigCmd : public Cmd {
public:
  ConfigCmd() {
  }
  virtual void Do();
private:
  std::vector<std::string> config_args_v_;
  virtual void DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
  void ConfigGet(std::string &ret);
  void ConfigSet(std::string &ret);
  void ConfigRewrite(std::string &ret);
};
#endif
