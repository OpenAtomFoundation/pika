// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_zset.h"
#include "include/pika_slot_command.h"

#include <cstdint>

#include "pstd/include/pstd_string.h"

void ZAddCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZAdd);
    return;
  }
  size_t argc = argv_.size();
  if (argc % 2 == 1) {
    res_.SetRes(CmdRes::kSyntaxErr);
    return;
  }
  key_ = argv_[1];
  score_members.clear();
  double score;
  size_t index = 2;
  for (; index < argc; index += 2) {
    if (pstd::string2d(argv_[index].data(), argv_[index].size(), &score) == 0) {
      res_.SetRes(CmdRes::kInvalidFloat);
      return;
    }
    score_members.push_back({score, argv_[index + 1]});
  }
}

void ZAddCmd::Do(std::shared_ptr<Slot> slot) {
  int32_t count = 0;
  rocksdb::Status s = slot->db()->ZAdd(key_, score_members, &count);
  if (s.ok()) {
    res_.AppendInteger(count);
    AddSlotKey("z", key_, slot);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void ZCardCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZCard);
    return;
  }
  key_ = argv_[1];
}

void ZCardCmd::Do(std::shared_ptr<Slot> slot) {
  int32_t card = 0;
  rocksdb::Status s = slot->db()->ZCard(key_, &card);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendInteger(card);
  } else {
    res_.SetRes(CmdRes::kErrOther, "zcard error");
  }
}

void ZScanCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZScan);
    return;
  }
  key_ = argv_[1];
  if (pstd::string2int(argv_[2].data(), argv_[2].size(), &cursor_) == 0) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZScan);
    return;
  }
  size_t argc = argv_.size();
  size_t index = 3;
  while (index < argc) {
    std::string opt = argv_[index];
    if ((strcasecmp(opt.data(), "match") == 0) || (strcasecmp(opt.data(), "count") == 0)) {
      index++;
      if (index >= argc) {
        res_.SetRes(CmdRes::kSyntaxErr);
        return;
      }
      if (strcasecmp(opt.data(), "match") == 0) {
        pattern_ = argv_[index];
      } else if (pstd::string2int(argv_[index].data(), argv_[index].size(), &count_) == 0) {
        res_.SetRes(CmdRes::kInvalidInt);
        return;
      }
    } else {
      res_.SetRes(CmdRes::kSyntaxErr);
      return;
    }
    index++;
  }
  if (count_ < 0) {
    res_.SetRes(CmdRes::kSyntaxErr);
    return;
  }
}

void ZScanCmd::Do(std::shared_ptr<Slot> slot) {
  int64_t next_cursor = 0;
  std::vector<storage::ScoreMember> score_members;
  rocksdb::Status s = slot->db()->ZScan(key_, cursor_, pattern_, count_, &score_members, &next_cursor);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendContent("*2");
    char buf[32];
    int64_t len = pstd::ll2string(buf, sizeof(buf), next_cursor);
    res_.AppendStringLen(len);
    res_.AppendContent(buf);

    res_.AppendArrayLen(score_members.size() * 2);
    for (const auto& score_member : score_members) {
      res_.AppendString(score_member.member);

      len = pstd::d2string(buf, sizeof(buf), score_member.score);
      res_.AppendStringLen(len);
      res_.AppendContent(buf);
    }
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void ZIncrbyCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZIncrby);
    return;
  }
  key_ = argv_[1];
  if (pstd::string2d(argv_[2].data(), argv_[2].size(), &by_) == 0) {
    res_.SetRes(CmdRes::kInvalidFloat);
    return;
  }
  member_ = argv_[3];
}

void ZIncrbyCmd::Do(std::shared_ptr<Slot> slot) {
  double score = 0;
  rocksdb::Status s = slot->db()->ZIncrby(key_, member_, by_, &score);
  if (s.ok()) {
    char buf[32];
    int64_t len = pstd::d2string(buf, sizeof(buf), score);
    res_.AppendStringLen(len);
    res_.AppendContent(buf);
    AddSlotKey("z", key_, slot);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void ZsetRangeParentCmd::DoInitial() {
  if (argv_.size() == 5 && (strcasecmp(argv_[4].data(), "withscores") == 0)) {
    is_ws_ = true;
  } else if (argv_.size() != 4) {
    res_.SetRes(CmdRes::kSyntaxErr);
    return;
  }
  key_ = argv_[1];
  if (pstd::string2int(argv_[2].data(), argv_[2].size(), &start_) == 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  if (pstd::string2int(argv_[3].data(), argv_[3].size(), &stop_) == 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
}

void ZRangeCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZRange);
    return;
  }
  ZsetRangeParentCmd::DoInitial();
}

void ZRangeCmd::Do(std::shared_ptr<Slot> slot) {
  std::vector<storage::ScoreMember> score_members;
  rocksdb::Status s = slot->db()->ZRange(key_, start_, stop_, &score_members);
  if (s.ok() || s.IsNotFound()) {
    if (is_ws_) {
      char buf[32];
      int64_t len;
      res_.AppendArrayLen(score_members.size() * 2);
      for (const auto& sm : score_members) {
        res_.AppendStringLen(sm.member.size());
        res_.AppendContent(sm.member);
        len = pstd::d2string(buf, sizeof(buf), sm.score);
        res_.AppendStringLen(len);
        res_.AppendContent(buf);
      }
    } else {
      res_.AppendArrayLen(score_members.size());
      for (const auto& sm : score_members) {
        res_.AppendStringLen(sm.member.size());
        res_.AppendContent(sm.member);
      }
    }
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void ZRevrangeCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZRevrange);
    return;
  }
  ZsetRangeParentCmd::DoInitial();
}

void ZRevrangeCmd::Do(std::shared_ptr<Slot> slot) {
  std::vector<storage::ScoreMember> score_members;
  rocksdb::Status s = slot->db()->ZRevrange(key_, start_, stop_, &score_members);
  if (s.ok() || s.IsNotFound()) {
    if (is_ws_) {
      char buf[32];
      int64_t len;
      res_.AppendArrayLen(score_members.size() * 2);
      for (const auto& sm : score_members) {
        res_.AppendStringLen(sm.member.size());
        res_.AppendContent(sm.member);
        len = pstd::d2string(buf, sizeof(buf), sm.score);
        res_.AppendStringLen(len);
        res_.AppendContent(buf);
      }
    } else {
      res_.AppendArrayLen(score_members.size());
      for (const auto& sm : score_members) {
        res_.AppendStringLen(sm.member.size());
        res_.AppendContent(sm.member);
      }
    }
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

int32_t DoScoreStrRange(std::string begin_score, std::string end_score, bool* left_close, bool* right_close,
                        double* min_score, double* max_score) {
  if (!begin_score.empty() && begin_score.at(0) == '(') {
    *left_close = false;
    begin_score.erase(begin_score.begin());
  }
  if (begin_score == "-inf") {
    *min_score = storage::ZSET_SCORE_MIN;
  } else if (begin_score == "inf" || begin_score == "+inf") {
    *min_score = storage::ZSET_SCORE_MAX;
  } else if (pstd::string2d(begin_score.data(), begin_score.size(), min_score) == 0) {
    return -1;
  }

  if (!end_score.empty() && end_score.at(0) == '(') {
    *right_close = false;
    end_score.erase(end_score.begin());
  }
  if (end_score == "+inf" || end_score == "inf") {
    *max_score = storage::ZSET_SCORE_MAX;
  } else if (end_score == "-inf") {
    *max_score = storage::ZSET_SCORE_MIN;
  } else if (pstd::string2d(end_score.data(), end_score.size(), max_score) == 0) {
    return -1;
  }
  return 0;
}

static void FitLimit(int64_t& count, int64_t& offset, const int64_t size) {
  count = count >= 0 ? count : size;
  offset = (offset >= 0 && offset < size) ? offset : size;
  count = (offset + count < size) ? count : size - offset;
}

void ZsetRangebyscoreParentCmd::DoInitial() {
  key_ = argv_[1];
  int32_t ret = DoScoreStrRange(argv_[2], argv_[3], &left_close_, &right_close_, &min_score_, &max_score_);
  if (ret == -1) {
    res_.SetRes(CmdRes::kErrOther, "min or max is not a float");
    return;
  }
  size_t argc = argv_.size();
  if (argc < 5) {
    return;
  }
  size_t index = 4;
  while (index < argc) {
    if (strcasecmp(argv_[index].data(), "withscores") == 0) {
      with_scores_ = true;
    } else if (strcasecmp(argv_[index].data(), "limit") == 0) {
      if (index + 3 > argc) {
        res_.SetRes(CmdRes::kSyntaxErr);
        return;
      }
      index++;
      if (pstd::string2int(argv_[index].data(), argv_[index].size(), &offset_) == 0) {
        res_.SetRes(CmdRes::kInvalidInt);
        return;
      }
      index++;
      if (pstd::string2int(argv_[index].data(), argv_[index].size(), &count_) == 0) {
        res_.SetRes(CmdRes::kInvalidInt);
        return;
      }
    } else {
      res_.SetRes(CmdRes::kSyntaxErr);
      return;
    }
    index++;
  }
}

void ZRangebyscoreCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZRangebyscore);
    return;
  }
  ZsetRangebyscoreParentCmd::DoInitial();
}

void ZRangebyscoreCmd::Do(std::shared_ptr<Slot> slot) {
  if (min_score_ == storage::ZSET_SCORE_MAX || max_score_ == storage::ZSET_SCORE_MIN) {
    res_.AppendContent("*0");
    return;
  }
  std::vector<storage::ScoreMember> score_members;
  rocksdb::Status s =
      slot->db()->ZRangebyscore(key_, min_score_, max_score_, left_close_, right_close_, &score_members);
  if (!s.ok() && !s.IsNotFound()) {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;
  }
  FitLimit(count_, offset_, score_members.size());
  size_t index = offset_;
  size_t end = offset_ + count_;
  if (with_scores_) {
    char buf[32];
    int64_t len;
    res_.AppendArrayLen(count_ * 2);
    for (; index < end; index++) {
      res_.AppendStringLen(score_members[index].member.size());
      res_.AppendContent(score_members[index].member);
      len = pstd::d2string(buf, sizeof(buf), score_members[index].score);
      res_.AppendStringLen(len);
      res_.AppendContent(buf);
    }
  } else {
    res_.AppendArrayLen(count_);
    for (; index < end; index++) {
      res_.AppendStringLen(score_members[index].member.size());
      res_.AppendContent(score_members[index].member);
    }
  }
}

void ZRevrangebyscoreCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZRevrangebyscore);
    return;
  }
  ZsetRangebyscoreParentCmd::DoInitial();
  double tmp_score;
  tmp_score = min_score_;
  min_score_ = max_score_;
  max_score_ = tmp_score;

  bool tmp_close;
  tmp_close = left_close_;
  left_close_ = right_close_;
  right_close_ = tmp_close;
}

void ZRevrangebyscoreCmd::Do(std::shared_ptr<Slot> slot) {
  if (min_score_ == storage::ZSET_SCORE_MAX || max_score_ == storage::ZSET_SCORE_MIN) {
    res_.AppendContent("*0");
    return;
  }
  std::vector<storage::ScoreMember> score_members;
  rocksdb::Status s =
      slot->db()->ZRevrangebyscore(key_, min_score_, max_score_, left_close_, right_close_, &score_members);
  if (!s.ok() && !s.IsNotFound()) {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;
  }
  FitLimit(count_, offset_, score_members.size());
  int64_t index = offset_;
  int64_t end = offset_ + count_;
  if (with_scores_) {
    char buf[32];
    int64_t len;
    res_.AppendArrayLen(count_ * 2);
    for (; index < end; index++) {
      res_.AppendStringLen(score_members[index].member.size());
      res_.AppendContent(score_members[index].member);
      len = pstd::d2string(buf, sizeof(buf), score_members[index].score);
      res_.AppendStringLen(len);
      res_.AppendContent(buf);
    }
  } else {
    res_.AppendArrayLen(count_);
    for (; index < end; index++) {
      res_.AppendStringLen(score_members[index].member.size());
      res_.AppendContent(score_members[index].member);
    }
  }
}

void ZCountCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZCount);
    return;
  }
  key_ = argv_[1];
  int32_t ret = DoScoreStrRange(argv_[2], argv_[3], &left_close_, &right_close_, &min_score_, &max_score_);
  if (ret == -1) {
    res_.SetRes(CmdRes::kErrOther, "min or max is not a float");
    return;
  }
}

void ZCountCmd::Do(std::shared_ptr<Slot> slot) {
  if (min_score_ == storage::ZSET_SCORE_MAX || max_score_ == storage::ZSET_SCORE_MIN) {
    res_.AppendContent("*0");
    return;
  }

  int32_t count = 0;
  rocksdb::Status s = slot->db()->ZCount(key_, min_score_, max_score_, left_close_, right_close_, &count);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendInteger(count);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void ZRemCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZRem);
    return;
  }
  key_ = argv_[1];
  auto iter = argv_.begin() + 2;
  members_.assign(iter, argv_.end());
}

void ZRemCmd::Do(std::shared_ptr<Slot> slot) {
  int32_t count = 0;
  rocksdb::Status s = slot->db()->ZRem(key_, members_, &count);
  if (s.ok() || s.IsNotFound()) {
    AddSlotKey("z", key_, slot);
    res_.AppendInteger(count);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void ZsetUIstoreParentCmd::DoInitial() {
  dest_key_ = argv_[1];
  if (pstd::string2int(argv_[2].data(), argv_[2].size(), &num_keys_) == 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  if (num_keys_ < 1) {
    res_.SetRes(CmdRes::kErrOther, "at least 1 input key is needed for ZUNIONSTORE/ZINTERSTORE");
    return;
  }
  int argc = argv_.size();
  if (argc < num_keys_ + 3) {
    res_.SetRes(CmdRes::kSyntaxErr);
    return;
  }
  keys_.assign(argv_.begin() + 3, argv_.begin() + 3 + num_keys_);
  weights_.assign(num_keys_, 1);
  int index = num_keys_ + 3;
  while (index < argc) {
    if (strcasecmp(argv_[index].data(), "weights") == 0) {
      index++;
      if (argc < index + num_keys_) {
        res_.SetRes(CmdRes::kSyntaxErr);
        return;
      }
      double weight;
      int base = index;
      for (; index < base + num_keys_; index++) {
        if (pstd::string2d(argv_[index].data(), argv_[index].size(), &weight) == 0) {
          res_.SetRes(CmdRes::kErrOther, "weight value is not a float");
          return;
        }
        weights_[index - base] = weight;
      }
    } else if (strcasecmp(argv_[index].data(), "aggregate") == 0) {
      index++;
      if (argc < index + 1) {
        res_.SetRes(CmdRes::kSyntaxErr);
        return;
      }
      if (strcasecmp(argv_[index].data(), "sum") == 0) {
        aggregate_ = storage::SUM;
      } else if (strcasecmp(argv_[index].data(), "min") == 0) {
        aggregate_ = storage::MIN;
      } else if (strcasecmp(argv_[index].data(), "max") == 0) {
        aggregate_ = storage::MAX;
      } else {
        res_.SetRes(CmdRes::kSyntaxErr);
        return;
      }
      index++;
    } else {
      res_.SetRes(CmdRes::kSyntaxErr);
      return;
    }
  }
}

void ZUnionstoreCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZUnionstore);
    return;
  }
  ZsetUIstoreParentCmd::DoInitial();
}

void ZUnionstoreCmd::Do(std::shared_ptr<Slot> slot) {
  int32_t count = 0;
  rocksdb::Status s = slot->db()->ZUnionstore(dest_key_, keys_, weights_, aggregate_, value_to_dest_, &count);
  if (s.ok()) {
    res_.AppendInteger(count);
    AddSlotKey("z", dest_key_, slot);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void ZUnionstoreCmd::DoBinlog(const std::shared_ptr<SyncMasterSlot>& slot) {
  PikaCmdArgsType del_args;
  del_args.emplace_back("del");
  del_args.emplace_back(dest_key_);
  del_cmd_->Initial(std::move(del_args), db_name_);
  del_cmd_->SetConn(GetConn());
  del_cmd_->SetResp(resp_.lock());
  del_cmd_->DoBinlog(slot);

  if(value_to_dest_.empty()){
    // The union operation got an empty set, only use del to simulate overwrite the dest_key with empty set
    return;
  }

  PikaCmdArgsType initial_args;
  initial_args.emplace_back("zadd");
  initial_args.emplace_back(dest_key_);
  auto first_pair = value_to_dest_.begin();
  char buf[32];
  int64_t d_len = pstd::d2string(buf, sizeof(buf), first_pair->second);
  initial_args.emplace_back(buf);
  initial_args.emplace_back(first_pair->first);
  value_to_dest_.erase(value_to_dest_.begin());
  zadd_cmd_->Initial(std::move(initial_args), db_name_);
  zadd_cmd_->SetConn(GetConn());
  zadd_cmd_->SetResp(resp_.lock());

  auto& zadd_argv = zadd_cmd_->argv();
  size_t data_size = d_len + zadd_argv[3].size();
  constexpr size_t max_binlog_size = 131072; //128KB
  for(auto it = value_to_dest_.begin(); it != value_to_dest_.end(); it++){
    if(data_size >= max_binlog_size) {
      // If the binlog has reached the size of 128KB. (131,072 bytes = 128KB)
      zadd_cmd_->DoBinlog(slot);
      zadd_argv.clear();
      zadd_argv.emplace_back("zadd");
      zadd_argv.emplace_back(dest_key_);
      data_size = 0;
    }
    d_len = pstd::d2string(buf, sizeof(buf), it->second);
    zadd_argv.emplace_back(buf);
    zadd_argv.emplace_back(it->first);
    data_size += (d_len + it->first.size());
  }
  zadd_cmd_->DoBinlog(slot);
}

void ZInterstoreCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZInterstore);
    return;
  }
  ZsetUIstoreParentCmd::DoInitial();
}

void ZInterstoreCmd::Do(std::shared_ptr<Slot> slot) {
  int32_t count = 0;
  rocksdb::Status s = slot->db()->ZInterstore(dest_key_, keys_, weights_, aggregate_, value_to_dest_, &count);
  if (s.ok()) {
    res_.AppendInteger(count);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void ZInterstoreCmd::DoBinlog(const std::shared_ptr<SyncMasterSlot>& slot) {
  PikaCmdArgsType del_args;
  del_args.emplace_back("del");
  del_args.emplace_back(dest_key_);
  del_cmd_->Initial(std::move(del_args), db_name_);
  del_cmd_->SetConn(GetConn());
  del_cmd_->SetResp(resp_.lock());
  del_cmd_->DoBinlog(slot);

  if(value_to_dest_.size() == 0){
    //The inter operation got an empty set, just exec del to simulate overwrite an empty set to dest_key
    return;
  }

  PikaCmdArgsType initial_args;
  initial_args.emplace_back("zadd");
  initial_args.emplace_back(dest_key_);
  char buf[32];
  int64_t d_len = pstd::d2string(buf, sizeof(buf), value_to_dest_[0].score);
  initial_args.emplace_back(buf);
  initial_args.emplace_back(value_to_dest_[0].member);
  zadd_cmd_->Initial(std::move(initial_args), db_name_);
  zadd_cmd_->SetConn(GetConn());
  zadd_cmd_->SetResp(resp_.lock());

  auto& zadd_argv = zadd_cmd_->argv();
  size_t data_size = d_len + value_to_dest_[0].member.size();
  constexpr size_t max_binlog_size = 131072; //128KB
  for(int i = 1; i < value_to_dest_.size(); i++){
    if(data_size >= max_binlog_size){
      // If the binlog has reached the size of 128KB. (131,072 bytes = 128KB)
      zadd_cmd_->DoBinlog(slot);
      zadd_argv.clear();
      zadd_argv.emplace_back("zadd");
      zadd_argv.emplace_back(dest_key_);
      data_size = 0;
    }
    d_len = pstd::d2string(buf, sizeof(buf), value_to_dest_[i].score);
    zadd_argv.emplace_back(buf);
    zadd_argv.emplace_back(value_to_dest_[i].member);
    data_size += (value_to_dest_[i].member.size() + d_len);
  }
  zadd_cmd_->DoBinlog(slot);
}

void ZsetRankParentCmd::DoInitial() {
  key_ = argv_[1];
  member_ = argv_[2];
}

void ZRankCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZRank);
    return;
  }
  ZsetRankParentCmd::DoInitial();
}

void ZRankCmd::Do(std::shared_ptr<Slot> slot) {
  int32_t rank = 0;
  rocksdb::Status s = slot->db()->ZRank(key_, member_, &rank);
  if (s.ok()) {
    res_.AppendInteger(rank);
  } else if (s.IsNotFound()) {
    res_.AppendContent("$-1");
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void ZRevrankCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZRevrank);
    return;
  }
  ZsetRankParentCmd::DoInitial();
}

void ZRevrankCmd::Do(std::shared_ptr<Slot> slot) {
  int32_t revrank = 0;
  rocksdb::Status s = slot->db()->ZRevrank(key_, member_, &revrank);
  if (s.ok()) {
    res_.AppendInteger(revrank);
  } else if (s.IsNotFound()) {
    res_.AppendContent("$-1");
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void ZScoreCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZScore);
    return;
  }
  key_ = argv_[1];
  member_ = argv_[2];
}

void ZScoreCmd::Do(std::shared_ptr<Slot> slot) {
  double score = 0;
  rocksdb::Status s = slot->db()->ZScore(key_, member_, &score);
  if (s.ok()) {
    char buf[32];
    int64_t len = pstd::d2string(buf, sizeof(buf), score);
    res_.AppendStringLen(len);
    res_.AppendContent(buf);
  } else if (s.IsNotFound()) {
    res_.AppendContent("$-1");
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

static int32_t DoMemberRange(const std::string& raw_min_member, const std::string& raw_max_member, bool* left_close,
                             bool* right_close, std::string* min_member, std::string* max_member) {
  if (raw_min_member == "-") {
    *min_member = "-";
  } else if (raw_min_member == "+") {
    *min_member = "+";
  } else {
    if (!raw_min_member.empty() && raw_min_member.at(0) == '(') {
      *left_close = false;
    } else if (!raw_min_member.empty() && raw_min_member.at(0) == '[') {
      *left_close = true;
    } else {
      return -1;
    }
    min_member->assign(raw_min_member.begin() + 1, raw_min_member.end());
  }

  if (raw_max_member == "+") {
    *max_member = "+";
  } else if (raw_max_member == "-") {
    *max_member = "-";
  } else {
    if (!raw_max_member.empty() && raw_max_member.at(0) == '(') {
      *right_close = false;
    } else if (!raw_max_member.empty() && raw_max_member.at(0) == '[') {
      *right_close = true;
    } else {
      return -1;
    }
    max_member->assign(raw_max_member.begin() + 1, raw_max_member.end());
  }
  return 0;
}

void ZsetRangebylexParentCmd::DoInitial() {
  key_ = argv_[1];
  int32_t ret = DoMemberRange(argv_[2], argv_[3], &left_close_, &right_close_, &min_member_, &max_member_);
  if (ret == -1) {
    res_.SetRes(CmdRes::kErrOther, "min or max not valid string range item");
    return;
  }
  size_t argc = argv_.size();
  if (argc == 4) {
    return;
  } else if (argc != 7 || strcasecmp(argv_[4].data(), "limit") != 0) {
    res_.SetRes(CmdRes::kSyntaxErr);
    return;
  }
  if (pstd::string2int(argv_[5].data(), argv_[5].size(), &offset_) == 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  if (pstd::string2int(argv_[6].data(), argv_[6].size(), &count_) == 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
}

void ZRangebylexCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZRangebylex);
    return;
  }
  ZsetRangebylexParentCmd::DoInitial();
}

void ZRangebylexCmd::Do(std::shared_ptr<Slot> slot) {
  if (min_member_ == "+" || max_member_ == "-") {
    res_.AppendContent("*0");
    return;
  }
  std::vector<std::string> members;
  rocksdb::Status s = slot->db()->ZRangebylex(key_, min_member_, max_member_, left_close_, right_close_, &members);
  if (!s.ok() && !s.IsNotFound()) {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;
  }
  FitLimit(count_, offset_, members.size());

  res_.AppendArrayLen(count_);
  size_t index = offset_;
  size_t end = offset_ + count_;
  for (; index < end; index++) {
    res_.AppendStringLen(members[index].size());
    res_.AppendContent(members[index]);
  }
}

void ZRevrangebylexCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZRevrangebylex);
    return;
  }
  ZsetRangebylexParentCmd::DoInitial();

  std::string tmp_s;
  tmp_s = min_member_;
  min_member_ = max_member_;
  max_member_ = tmp_s;

  bool tmp_b;
  tmp_b = left_close_;
  left_close_ = right_close_;
  right_close_ = tmp_b;
}

void ZRevrangebylexCmd::Do(std::shared_ptr<Slot> slot) {
  if (min_member_ == "+" || max_member_ == "-") {
    res_.AppendContent("*0");
    return;
  }
  std::vector<std::string> members;
  rocksdb::Status s = slot->db()->ZRangebylex(key_, min_member_, max_member_, left_close_, right_close_, &members);
  if (!s.ok() && !s.IsNotFound()) {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;
  }
  FitLimit(count_, offset_, members.size());

  res_.AppendArrayLen(count_);
  int64_t index = members.size() - 1 - offset_;
  int64_t end = index - count_;
  for (; index > end; index--) {
    res_.AppendStringLen(members[index].size());
    res_.AppendContent(members[index]);
  }
}

void ZLexcountCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZLexcount);
    return;
  }
  key_ = argv_[1];
  int32_t ret = DoMemberRange(argv_[2], argv_[3], &left_close_, &right_close_, &min_member_, &max_member_);
  if (ret == -1) {
    res_.SetRes(CmdRes::kErrOther, "min or max not valid string range item");
    return;
  }
}

void ZLexcountCmd::Do(std::shared_ptr<Slot> slot) {
  if (min_member_ == "+" || max_member_ == "-") {
    res_.AppendContent(":0");
    return;
  }
  int32_t count = 0;
  rocksdb::Status s = slot->db()->ZLexcount(key_, min_member_, max_member_, left_close_, right_close_, &count);
  if (!s.ok() && !s.IsNotFound()) {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;
  }
  res_.AppendInteger(count);
}

void ZRemrangebyrankCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZRemrangebyrank);
    return;
  }
  key_ = argv_[1];
  if (pstd::string2int(argv_[2].data(), argv_[2].size(), &start_rank_) == 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  if (pstd::string2int(argv_[3].data(), argv_[3].size(), &stop_rank_) == 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
}

void ZRemrangebyrankCmd::Do(std::shared_ptr<Slot> slot) {
  int32_t count = 0;
  rocksdb::Status s = slot->db()->ZRemrangebyrank(key_, start_rank_, stop_rank_, &count);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendInteger(count);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void ZRemrangebyscoreCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZRemrangebyscore);
    return;
  }
  key_ = argv_[1];
  int32_t ret = DoScoreStrRange(argv_[2], argv_[3], &left_close_, &right_close_, &min_score_, &max_score_);
  if (ret == -1) {
    res_.SetRes(CmdRes::kErrOther, "min or max is not a float");
    return;
  }
}

void ZRemrangebyscoreCmd::Do(std::shared_ptr<Slot> slot) {
  if (min_score_ == storage::ZSET_SCORE_MAX || max_score_ == storage::ZSET_SCORE_MIN) {
    res_.AppendContent(":0");
    return;
  }
  int32_t count = 0;
  rocksdb::Status s =
      slot->db()->ZRemrangebyscore(key_, min_score_, max_score_, left_close_, right_close_, &count);
  if (!s.ok() && !s.IsNotFound()) {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;
  }
  res_.AppendInteger(count);
}

void ZRemrangebylexCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZRemrangebylex);
    return;
  }
  key_ = argv_[1];
  int32_t ret = DoMemberRange(argv_[2], argv_[3], &left_close_, &right_close_, &min_member_, &max_member_);
  if (ret == -1) {
    res_.SetRes(CmdRes::kErrOther, "min or max not valid string range item");
    return;
  }
}

void ZRemrangebylexCmd::Do(std::shared_ptr<Slot> slot) {
  if (min_member_ == "+" || max_member_ == "-") {
    res_.AppendContent("*0");
    return;
  }
  int32_t count = 0;
  rocksdb::Status s =
      slot->db()->ZRemrangebylex(key_, min_member_, max_member_, left_close_, right_close_, &count);
  if (!s.ok() && !s.IsNotFound()) {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;
  }
  res_.AppendInteger(count);
}

void ZPopmaxCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZPopmax);
    return;
  }
  key_ = argv_[1];
  if (argv_.size() == 2) {
    count_ = 1;
    return;
  }
  if (pstd::string2int(argv_[2].data(), argv_[2].size(), static_cast<int64_t *>(&count_)) == 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
}

void ZPopmaxCmd::Do(std::shared_ptr<Slot> slot) {
  std::vector<storage::ScoreMember> score_members;
  rocksdb::Status s = slot->db()->ZPopMax(key_, count_, &score_members);
  if (s.ok() || s.IsNotFound()) {
    char buf[32];
    int64_t len;
    res_.AppendArrayLen(score_members.size() * 2);
    for (const auto& sm : score_members) {
      res_.AppendString(sm.member);
      len = pstd::d2string(buf, sizeof(buf), sm.score);
      res_.AppendStringLen(len);
      res_.AppendContent(buf);
    }
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void ZPopminCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZPopmin);
    return;
  }
  key_ = argv_[1];
  if (argv_.size() == 2) {
    count_ = 1;
    return;
  }
  if (pstd::string2int(argv_[2].data(), argv_[2].size(), static_cast<int64_t *>(&count_)) == 0) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
}

void ZPopminCmd::Do(std::shared_ptr<Slot> slot) {
  std::vector<storage::ScoreMember> score_members;
  rocksdb::Status s = slot->db()->ZPopMin(key_, count_, &score_members);
  if (s.ok() || s.IsNotFound()) {
    char buf[32];
    int64_t len;
    res_.AppendArrayLen(score_members.size() * 2);
    for (const auto& sm : score_members) {
      res_.AppendString(sm.member);
      len = pstd::d2string(buf, sizeof(buf), sm.score);
      res_.AppendStringLen(len);
      res_.AppendContent(buf);
    }
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}
