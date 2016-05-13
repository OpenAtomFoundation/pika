#ifndef PIKA_ZSET_H_
#define PIKA_ZSET_H_
#include "pika_command.h"
#include "nemo.h"


/*
 * zset
 */
class ZAddCmd : public Cmd {
public:
  ZAddCmd() {}
  virtual void Do();
private:
  std::string key_;
  std::vector<nemo::SM> sms_v_;
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class ZCardCmd : public Cmd {
public:
  ZCardCmd() {}
  virtual void Do();
private:
  std::string key_;
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class ZScanCmd : public Cmd {
public:
  ZScanCmd() : pattern_("*"), count_(10) {}
  virtual void Do();
private:
  std::string key_, pattern_;
  int64_t cursor_, count_;
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
  virtual void Clear() {
    pattern_ = "*";
    count_ = 10;
  }
};

class ZIncrbyCmd : public Cmd {
public:
  ZIncrbyCmd() {}
  virtual void Do();
private:
  std::string key_, member_;
  double by_;
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class ZsetRangeParentCmd : public Cmd {
public:
  ZsetRangeParentCmd() : is_ws_(false) {}
protected:
  std::string key_;
  int64_t start_, stop_;
  bool is_ws_;
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
  virtual void Clear() {
    is_ws_ = false;
  }
};

class ZRangeCmd : public ZsetRangeParentCmd {
public:
  virtual void Do();
private:
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class ZRevrangeCmd : public ZsetRangeParentCmd {
public:
  virtual void Do();
private:
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class ZsetRangebyscoreParentCmd : public Cmd {
public:
  ZsetRangebyscoreParentCmd() : is_lo_(false), is_ro_(false), is_ws_(false), offset_(0), count_(-1) {}
protected:
  std::string key_;
  double min_score_, max_score_;
  bool is_lo_, is_ro_, is_ws_;
  int64_t offset_, count_;
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
  virtual void Clear() {
    is_lo_ = is_ro_ = is_ws_ = false;
    offset_ = 0;
    count_ = -1;
  }
};

class ZRangebyscoreCmd : public ZsetRangebyscoreParentCmd {
public:
  virtual void Do();
private:
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class ZRevrangebyscoreCmd : public ZsetRangebyscoreParentCmd {
public:
  virtual void Do();
private:
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class ZCountCmd : public Cmd {
public:
  ZCountCmd() : is_lo_(false), is_ro_(false) {}
  virtual void Do();
private:
  std::string key_;
  double min_score_, max_score_;
  bool is_lo_, is_ro_;
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
  virtual void Clear() {
    is_lo_ = false;
    is_ro_ = false;
  }
};

class ZRemCmd : public Cmd {
public:
  ZRemCmd() {}
  virtual void Do();
private:
  std::string key_;
  std::vector<std::string> members_;
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class ZsetUIstoreParentCmd : public Cmd {
public:
  ZsetUIstoreParentCmd() : aggregate_(nemo::SUM) {}
protected:
  std::string dest_key_;
  int64_t num_keys_;
  nemo::Aggregate aggregate_;
  std::vector<std::string> keys_;
  std::vector<double> weights_;
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
  virtual void Clear() {
    aggregate_ = nemo::SUM;
  }
};

class ZUnionstoreCmd : public ZsetUIstoreParentCmd {
public:
  virtual void Do();
private:
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class ZInterstoreCmd : public ZsetUIstoreParentCmd {
public:
  virtual void Do();
private:
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class ZsetRankParentCmd : public Cmd {
public:
  ZsetRankParentCmd() {}
protected:
  std::string key_, member_;
  virtual void DoInitial(PikaCmdArgsType & argvs, const CmdInfo* const ptr_info);
};

class ZRankCmd : public ZsetRankParentCmd {
public:
  virtual void Do();
private:
  virtual void DoInitial(PikaCmdArgsType & argvs, const CmdInfo* const ptr_info);
};

class ZRevrankCmd : public ZsetRankParentCmd {
public:
  virtual void Do();
private:
  virtual void DoInitial(PikaCmdArgsType & argvs, const CmdInfo* const ptr_info);
};

class ZScoreCmd : public ZsetRankParentCmd {
public:
  ZScoreCmd() {}
  virtual void Do();
private:
  std::string key_, member_;
  virtual void DoInitial(PikaCmdArgsType & argvs, const CmdInfo* const ptr_info);
};


class ZsetRangebylexParentCmd : public Cmd {
public:
  ZsetRangebylexParentCmd() : offset_(0), count_(-1) {}
protected:
  std::string key_, min_member_, max_member_;
  bool is_lo_, is_ro_;
  int64_t offset_, count_;
  virtual void DoInitial(PikaCmdArgsType & argvs, const CmdInfo* const ptr_info);
  virtual void Clear() {
    offset_ = 0;
    count_ = -1;
  }
};

class ZRangebylexCmd : public ZsetRangebylexParentCmd {
public:
  virtual void Do();
private: 
  virtual void DoInitial(PikaCmdArgsType & argvs, const CmdInfo* const ptr_info);
};

class ZRevrangebylexCmd : public ZsetRangebylexParentCmd {
public:
  virtual void Do();
private: 
  virtual void DoInitial(PikaCmdArgsType & argvs, const CmdInfo* const ptr_info);
};

class ZLexcountCmd : public Cmd {
public:
  ZLexcountCmd() {}
  virtual void Do();
private:
  std::string key_, min_member_, max_member_;
  bool is_lo_, is_ro_;
  virtual void DoInitial(PikaCmdArgsType & argvs, const CmdInfo* const ptr_info);
};

class ZRemrangebyrankCmd : public Cmd {
public:
  ZRemrangebyrankCmd() {}
  virtual void Do();
private:
  std::string key_;
  int64_t start_rank_, stop_rank_;
  virtual void DoInitial(PikaCmdArgsType & argvs, const CmdInfo* const ptr_info);
};

class ZRemrangebyscoreCmd : public Cmd {
public:
  ZRemrangebyscoreCmd() : is_lo_(false), is_ro_(false) {}
  virtual void Do();
private:
  std::string key_;
  double min_score_, max_score_;
  bool is_lo_, is_ro_;
  virtual void DoInitial(PikaCmdArgsType & argvs, const CmdInfo* const ptr_info);
  virtual void Clear() {
    is_lo_ = false;
    is_ro_ = false;
  }
};

class ZRemrangebylexCmd : public Cmd {
public:
  ZRemrangebylexCmd() {}
  virtual void Do();
private:
  std::string key_;
  std::string min_member_, max_member_;
  bool is_lo_, is_ro_;
  virtual void DoInitial(PikaCmdArgsType & argvs, const CmdInfo* const ptr_info);
};
#endif
