#ifndef PIKA_BIT_H_
#define PIKA_BIT_H_
#include "pika_command.h"
#include "nemo.h"


/*
 * bitoperation
 */
class BitGetCmd : public Cmd {
public:
  BitGetCmd() {};
  virtual void Do();
private:
  std::string key_;
  int64_t  bit_offset_;
  virtual void Clear() {
    key_ = "";
    bit_offset_ = -1;
  }

  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class BitSetCmd : public Cmd {
public:
  BitSetCmd() {};
  virtual void Do();
private:
  std::string key_;
  int64_t  bit_offset_;
  int64_t  on_;
  virtual void Clear() {
    key_ = "";
    bit_offset_ = -1;
    on_ = -1;
  }
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class BitCountCmd : public Cmd {
public:
  BitCountCmd() {}
  virtual void Do();
private:
  std::string key_;
  bool  count_all_;
  int64_t  start_offset_;
  int64_t  end_offset_;
  virtual void Clear() {
    key_ = "";
    count_all_ = false;
    start_offset_ = -1;
    end_offset_ = -1;
  }
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class BitPosCmd : public Cmd {
public:
  BitPosCmd() {};
  virtual void Do();
private:
  std::string key_;
  bool  pos_all_;
  bool  endoffset_set_;
  int64_t bit_val_;
  int64_t  start_offset_;
  int64_t  end_offset_;
  virtual void Clear() {
    key_ = "";
    pos_all_ = false;
    endoffset_set_ = false;
    bit_val_ = -1;
    start_offset_ = -1;
    end_offset_ = -1;
  }

  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class BitOpCmd : public Cmd {
public:
  BitOpCmd() {};
  virtual void Do();
private:
  std::string dest_key_;
  std::vector<std::string> src_keys_;
  nemo::BitOpType op_;
  virtual void Clear() {
    dest_key_ = "";
    src_keys_.clear();
    op_ = nemo::kBitOpDefault;
  }

  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};
#endif
