// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_ZSET_H_
#define PIKA_ZSET_H_
#include "include/pika_command.h"
#include "nemo.h"
#include "blackwidow/blackwidow.h"


/*
 * zset
 */
class ZAddCmd : public Cmd {
public:
  ZAddCmd() {}
  virtual void Do();
private:
  std::string key_;
  std::vector<blackwidow::BlackWidow::ScoreMember> score_members;
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
  ZsetRangebyscoreParentCmd() : left_close_(true), right_close_(true), with_scores_(false), offset_(0), count_(-1) {}
protected:
  std::string key_;
  double min_score_, max_score_;
  bool left_close_, right_close_, with_scores_;
  int64_t offset_, count_;
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
  virtual void Clear() {
    left_close_ = right_close_ = true;
    with_scores_ = false;
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
  ZCountCmd() : left_close_(true), right_close_(true) {}
  virtual void Do();
private:
  std::string key_;
  double min_score_, max_score_;
  bool left_close_, right_close_;
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
  virtual void Clear() {
    left_close_ = true;
    right_close_ = true;
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
  ZsetUIstoreParentCmd() : aggregate_(blackwidow::BlackWidow::SUM) {}
protected:
  std::string dest_key_;
  int64_t num_keys_;
  blackwidow::BlackWidow::AGGREGATE aggregate_;
  std::vector<std::string> keys_;
  std::vector<double> weights_;
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
  virtual void Clear() {
    aggregate_ = blackwidow::BlackWidow::SUM;
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
  ZsetRangebylexParentCmd() : left_close_(true), right_close_(true), offset_(0), count_(-1) {}
protected:
  std::string key_, min_member_, max_member_;
  bool left_close_, right_close_;
  int64_t offset_, count_;
  virtual void DoInitial(PikaCmdArgsType & argvs, const CmdInfo* const ptr_info);
  virtual void Clear() {
    left_close_ = right_close_ = true;
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
  ZLexcountCmd() : left_close_(true), right_close_(true) {}
  virtual void Do();
private:
  std::string key_, min_member_, max_member_;
  bool left_close_, right_close_;
  virtual void DoInitial(PikaCmdArgsType & argvs, const CmdInfo* const ptr_info);
  virtual void Clear() {
    left_close_ = right_close_ = true;
  }
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
  ZRemrangebyscoreCmd() : left_close_(true), right_close_(true) {}
  virtual void Do();
private:
  std::string key_;
  double min_score_, max_score_;
  bool left_close_, right_close_;
  virtual void DoInitial(PikaCmdArgsType & argvs, const CmdInfo* const ptr_info);
  virtual void Clear() {
    left_close_ =  right_close_ = true;
  }
};

class ZRemrangebylexCmd : public Cmd {
public:
  ZRemrangebylexCmd() : left_close_(true), right_close_(true) {}
  virtual void Do();
private:
  std::string key_;
  std::string min_member_, max_member_;
  bool left_close_, right_close_;
  virtual void DoInitial(PikaCmdArgsType & argvs, const CmdInfo* const ptr_info);
  virtual void Clear() {
    left_close_ =  right_close_ = true;
  }
};
#endif
