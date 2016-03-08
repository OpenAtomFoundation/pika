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
  virtual void Initial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
private:
  std::string master_ip_;
  int64_t master_port_;
  bool is_noone_;
  bool have_offset_;
  int64_t filenum_;
  int64_t pro_offset_;
};

class TrysyncCmd : public Cmd {
public:
  TrysyncCmd() {
  }
  virtual void Do();
  virtual void Initial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
private:
  std::string slave_ip_;
  int64_t slave_port_;
  int64_t filenum_;
  int64_t pro_offset_;
};

#endif
