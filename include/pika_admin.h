#ifndef __PIKA_ADMIN_H__
#define __PIKA_ADMIN_H__
#include "pika_command.h"

/*
 * Admin
 */
class SlaveofCmd : public Cmd {
public:
  SlaveofCmd() : have_offset_(false),
  filenum_(0), pro_offset_(0) {
  }
  virtual void Do(PikaCmdArgsType &argvs);
private:
  std::string master_ip_;
  int64_t master_port_;
  bool have_offset_;
  int64_t filenum_;
  int64_t pro_offset_;
  virtual void Initial(PikaCmdArgsType &argvs);
};

#endif
