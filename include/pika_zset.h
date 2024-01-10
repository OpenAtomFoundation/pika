// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_ZSET_H_
#define PIKA_ZSET_H_

#include "storage/storage.h"

#include "include/acl.h"
#include "include/pika_command.h"
#include "include/pika_slot.h"
#include "pika_kv.h"

/*
 * zset
 */
class ZAddCmd : public Cmd {
 public:
  ZAddCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::SORTEDSET)) {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void DoUpdateCache(std::shared_ptr<Slot> slot = nullptr) override;
  void DoThroughDB(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new ZAddCmd(*this); }

 private:
  std::string key_;
  std::vector<storage::ScoreMember> score_members;
  rocksdb::Status s_;
  void DoInitial() override;
};

class ZCardCmd : public Cmd {
 public:
  ZCardCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::SORTEDSET)) {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void ReadCache(std::shared_ptr<Slot> slot = nullptr) override;
  void DoUpdateCache(std::shared_ptr<Slot> slot = nullptr) override;
  void DoThroughDB(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new ZCardCmd(*this); }

 private:
  std::string key_;
  void DoInitial() override;
};

class ZScanCmd : public Cmd {
 public:
  ZScanCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::SORTEDSET)), pattern_("*") {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new ZScanCmd(*this); }

 private:
  std::string key_, pattern_ = "*";
  int64_t cursor_ = 0, count_ = 10;
  void DoInitial() override;
  void Clear() override {
    pattern_ = "*";
    count_ = 10;
  }
};

class ZIncrbyCmd : public Cmd {
 public:
  ZIncrbyCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::SORTEDSET)) {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void DoUpdateCache(std::shared_ptr<Slot> slot = nullptr) override;
  void DoThroughDB(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new ZIncrbyCmd(*this); }
  double Score() { return score_; }

 private:
  std::string key_, member_;
  double by_ = .0f;
  double score_ = .0f;
  void DoInitial() override;
};

class ZsetRangeParentCmd : public Cmd {
 public:
  ZsetRangeParentCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::SORTEDSET)) {}

 protected:
  std::string key_;
  int64_t start_ = 0;
  int64_t stop_ = -1;
  bool is_ws_ = false;
  void DoInitial() override;
  void Clear() override { is_ws_ = false; }
};

class ZRangeCmd : public ZsetRangeParentCmd {
 public:
  ZRangeCmd(const std::string& name, int arity, uint32_t flag) : ZsetRangeParentCmd(name, arity, flag) {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void ReadCache(std::shared_ptr<Slot> slot = nullptr) override;
  void DoUpdateCache(std::shared_ptr<Slot> slot = nullptr) override;
  void DoThroughDB(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new ZRangeCmd(*this); }

 private:
  rocksdb::Status s_;
  void DoInitial() override;
};

class ZRevrangeCmd : public ZsetRangeParentCmd {
 public:
  ZRevrangeCmd(const std::string& name, int arity, uint32_t flag) : ZsetRangeParentCmd(name, arity, flag) {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void ReadCache(std::shared_ptr<Slot> slot = nullptr) override;
  void DoUpdateCache(std::shared_ptr<Slot> slot = nullptr) override;
  void DoThroughDB(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new ZRevrangeCmd(*this); }

 private:
  rocksdb::Status s_;
  void DoInitial() override;
};

class ZsetRangebyscoreParentCmd : public Cmd {
 public:
  ZsetRangebyscoreParentCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::SORTEDSET)) {}

  double MinScore() { return min_score_; }
  double MaxScore() { return max_score_; }
  bool LeftClose() { return left_close_; }
  bool RightClose() { return right_close_; }
  int64_t Offset() { return offset_; }
  int64_t Count() { return count_; }
 protected:
  std::string key_;
  std::string min_, max_;
  double min_score_ = 0, max_score_ = 0;
  bool left_close_ = true, right_close_ = true, with_scores_ = false;
  int64_t offset_ = 0, count_ = -1;
  void DoInitial() override;
  void Clear() override {
    left_close_ = right_close_ = true;
    with_scores_ = false;
    offset_ = 0;
    count_ = -1;
  }
};

class ZRangebyscoreCmd : public ZsetRangebyscoreParentCmd {
 public:
  ZRangebyscoreCmd(const std::string& name, int arity, uint32_t flag) : ZsetRangebyscoreParentCmd(name, arity, flag) {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void ReadCache(std::shared_ptr<Slot> slot = nullptr) override;
  void DoUpdateCache(std::shared_ptr<Slot> slot = nullptr) override;
  void DoThroughDB(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new ZRangebyscoreCmd(*this); }

 private:
  rocksdb::Status s_;
  void DoInitial() override;
};

class ZRevrangebyscoreCmd : public ZsetRangebyscoreParentCmd {
 public:
  ZRevrangebyscoreCmd(const std::string& name, int arity, uint32_t flag)
      : ZsetRangebyscoreParentCmd(name, arity, flag) {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void ReadCache(std::shared_ptr<Slot> slot = nullptr) override;
  void DoUpdateCache(std::shared_ptr<Slot> slot = nullptr) override;
  void DoThroughDB(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new ZRevrangebyscoreCmd(*this); }

 private:
  rocksdb::Status s_;
  void DoInitial() override;
};

class ZCountCmd : public Cmd {
 public:
  ZCountCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::SORTEDSET)) {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void ReadCache(std::shared_ptr<Slot> slot = nullptr) override;
  void DoUpdateCache(std::shared_ptr<Slot> slot = nullptr) override;
  void DoThroughDB(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new ZCountCmd(*this); }
  double MinScore() { return min_score_; }
  double MaxScore() { return max_score_; }
  bool LeftClose() { return left_close_; }
  bool RightClose() { return right_close_; }

 private:
  std::string key_;
  std::string min_ , max_;
  double min_score_ = 0, max_score_ = 0;
  bool left_close_ = true, right_close_ = true;
  rocksdb::Status s_;
  void DoInitial() override;
  void Clear() override {
    left_close_ = true;
    right_close_ = true;
  }
};

class ZRemCmd : public Cmd {
 public:
  ZRemCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::SORTEDSET)) {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void DoUpdateCache(std::shared_ptr<Slot> slot = nullptr) override;
  void DoThroughDB(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new ZRemCmd(*this); }

 private:
  std::string key_;
  std::vector<std::string> members_;
  int32_t deleted_ = 0;
  rocksdb::Status s_;
  void DoInitial() override;
};

class ZsetUIstoreParentCmd : public Cmd {
 public:
  ZsetUIstoreParentCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::SORTEDSET)) {
    zadd_cmd_ = std::make_unique<ZAddCmd>(kCmdNameZAdd, -4, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsZset);
    del_cmd_ = std::make_shared<DelCmd>(kCmdNameDel, -2, kCmdFlagsWrite | kCmdFlagsMultiSlot | kCmdFlagsKv);
  }
  ZsetUIstoreParentCmd(const ZsetUIstoreParentCmd& other)
      : Cmd(other),
        dest_key_(other.dest_key_),
        num_keys_(other.num_keys_),
        aggregate_(other.aggregate_),
        keys_(other.keys_),
        weights_(other.weights_) {
    zadd_cmd_ = std::make_unique<ZAddCmd>(kCmdNameZAdd, -4, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsZset);
    del_cmd_ = std::make_shared<DelCmd>(kCmdNameDel, -2, kCmdFlagsWrite | kCmdFlagsMultiSlot | kCmdFlagsKv);
  }

  std::vector<std::string> current_key() const override { return {dest_key_}; }

 protected:
  std::string dest_key_;
  int64_t num_keys_ = 0;
  storage::AGGREGATE aggregate_{storage::SUM};
  std::vector<std::string> keys_;
  std::vector<double> weights_;
  void DoInitial() override;
  void Clear() override { aggregate_ = storage::SUM; }
  // used for write binlog
  std::shared_ptr<Cmd> zadd_cmd_;
  std::shared_ptr<Cmd> del_cmd_;
};

class ZUnionstoreCmd : public ZsetUIstoreParentCmd {
 public:
  ZUnionstoreCmd(const std::string& name, int arity, uint32_t flag) : ZsetUIstoreParentCmd(name, arity, flag) {}
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void DoUpdateCache(std::shared_ptr<Slot> slot = nullptr) override;
  void DoThroughDB(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new ZUnionstoreCmd(*this); }

 private:
  void DoInitial() override;
  // used for write binlog
  std::map<std::string, double> value_to_dest_;
  rocksdb::Status s_;
  void DoBinlog(const std::shared_ptr<SyncMasterSlot>& slot) override;
};

class ZInterstoreCmd : public ZsetUIstoreParentCmd {
 public:
  ZInterstoreCmd(const std::string& name, int arity, uint32_t flag) : ZsetUIstoreParentCmd(name, arity, flag) {}
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void DoUpdateCache(std::shared_ptr<Slot> slot = nullptr) override;
  void DoThroughDB(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new ZInterstoreCmd(*this); }
  void DoBinlog(const std::shared_ptr<SyncMasterSlot>& slot) override;

 private:
  void DoInitial() override;
  rocksdb::Status s_;
  // used for write binlog
  std::vector<storage::ScoreMember> value_to_dest_;
};

class ZsetRankParentCmd : public Cmd {
 public:
  ZsetRankParentCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::SORTEDSET)) {}

 protected:
  std::string key_, member_;
  void DoInitial() override;
};

class ZRankCmd : public ZsetRankParentCmd {
 public:
  ZRankCmd(const std::string& name, int arity, uint32_t flag) : ZsetRankParentCmd(name, arity, flag) {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void ReadCache(std::shared_ptr<Slot> slot = nullptr) override;
  void DoUpdateCache(std::shared_ptr<Slot> slot = nullptr) override;
  void DoThroughDB(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new ZRankCmd(*this); }

 private:
  rocksdb::Status s_;
  void DoInitial() override;
};

class ZRevrankCmd : public ZsetRankParentCmd {
 public:
  ZRevrankCmd(const std::string& name, int arity, uint32_t flag) : ZsetRankParentCmd(name, arity, flag) {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void ReadCache(std::shared_ptr<Slot> slot = nullptr) override;
  void DoUpdateCache(std::shared_ptr<Slot> slot = nullptr) override;
  void DoThroughDB(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new ZRevrankCmd(*this); }

 private:
  rocksdb::Status s_;
  void DoInitial() override;
};

class ZScoreCmd : public ZsetRankParentCmd {
 public:
  ZScoreCmd(const std::string& name, int arity, uint32_t flag) : ZsetRankParentCmd(name, arity, flag) {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void ReadCache(std::shared_ptr<Slot> slot = nullptr) override;
  void DoUpdateCache(std::shared_ptr<Slot> slot = nullptr) override;
  void DoThroughDB(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new ZScoreCmd(*this); }

 private:
  std::string key_, member_;
  rocksdb::Status s_;
  void DoInitial() override;
};

class ZsetRangebylexParentCmd : public Cmd {
 public:
  ZsetRangebylexParentCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::SORTEDSET)) {}

 protected:
  std::string key_, min_member_, max_member_;
  std::string min_, max_;
  bool left_close_ = true, right_close_ = true;
  int64_t offset_ = 0, count_ = -1;
  void DoInitial() override;
  void Clear() override {
    left_close_ = right_close_ = true;
    offset_ = 0;
    count_ = -1;
  }
};

class ZRangebylexCmd : public ZsetRangebylexParentCmd {
 public:
  ZRangebylexCmd(const std::string& name, int arity, uint32_t flag) : ZsetRangebylexParentCmd(name, arity, flag) {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void ReadCache(std::shared_ptr<Slot> slot = nullptr) override;
  void DoUpdateCache(std::shared_ptr<Slot> slot = nullptr) override;
  void DoThroughDB(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new ZRangebylexCmd(*this); }

 private:
  rocksdb::Status s_;
  void DoInitial() override;
};

class ZRevrangebylexCmd : public ZsetRangebylexParentCmd {
 public:
  ZRevrangebylexCmd(const std::string& name, int arity, uint32_t flag) : ZsetRangebylexParentCmd(name, arity, flag) {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void ReadCache(std::shared_ptr<Slot> slot = nullptr) override;
  void DoUpdateCache(std::shared_ptr<Slot> slot = nullptr) override;
  void DoThroughDB(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new ZRevrangebylexCmd(*this); }

 private:
  void DoInitial() override;
  rocksdb::Status s_;
};

class ZLexcountCmd : public Cmd {
 public:
  ZLexcountCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::SORTEDSET)) {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void ReadCache(std::shared_ptr<Slot> slot = nullptr) override;
  void DoUpdateCache(std::shared_ptr<Slot> slot = nullptr) override;
  void DoThroughDB(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new ZLexcountCmd(*this); }

 private:
  std::string key_, min_member_, max_member_;
  std::string min_, max_;
  bool left_close_ = true, right_close_ = true;
  rocksdb::Status s_;
  void DoInitial() override;
  void Clear() override { left_close_ = right_close_ = true; }
};

class ZRemrangebyrankCmd : public Cmd {
 public:
  ZRemrangebyrankCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::SORTEDSET)) {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void DoUpdateCache(std::shared_ptr<Slot> slot = nullptr) override;
  void DoThroughDB(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new ZRemrangebyrankCmd(*this); }

 private:
  std::string key_, min_, max_;
  int64_t start_rank_ = 0, stop_rank_ = -1;
  int32_t ele_deleted_;
  rocksdb::Status s_;
  void DoInitial() override;
};

class ZRemrangebyscoreCmd : public Cmd {
 public:
  ZRemrangebyscoreCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::SORTEDSET)) {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void DoUpdateCache(std::shared_ptr<Slot> slot = nullptr) override;
  void DoThroughDB(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new ZRemrangebyscoreCmd(*this); }

 private:
  std::string key_, min_, max_;
  double min_score_ = 0, max_score_ = 0;
  bool left_close_ = true, right_close_ = true;
  rocksdb::Status s_;
  void DoInitial() override;
  void Clear() override { left_close_ = right_close_ = true; }
};

class ZRemrangebylexCmd : public Cmd {
 public:
  ZRemrangebylexCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::SORTEDSET)) {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void DoUpdateCache(std::shared_ptr<Slot> slot = nullptr) override;
  void DoThroughDB(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new ZRemrangebylexCmd(*this); }

 private:
  std::string key_, min_, max_;
  std::string min_member_, max_member_;
  bool left_close_ = true, right_close_ = true;
  rocksdb::Status s_;
  void DoInitial() override;
  void Clear() override { left_close_ = right_close_ = true; }
};

class ZPopmaxCmd : public Cmd {
 public:
  ZPopmaxCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::SORTEDSET)) {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.emplace_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new ZPopmaxCmd(*this); }

 private:
  void DoInitial() override;
  std::string key_;
  int64_t count_ = 0;
};

class ZPopminCmd : public Cmd {
 public:
  ZPopminCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::SORTEDSET)) {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new ZPopminCmd(*this); }

 private:
  void DoInitial() override;
  std::string key_;
  int64_t count_ = 0;
};

#endif
