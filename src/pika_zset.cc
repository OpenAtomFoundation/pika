// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_zset.h"

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
    if (!pstd::string2d(argv_[index].data(), argv_[index].size(), &score)) {
      res_.SetRes(CmdRes::kInvalidFloat);
      return;
    }
    score_members.push_back({score, argv_[index + 1]});
  }
  return;
}

void ZAddCmd::Do(std::shared_ptr<Partition> partition) {
  int32_t count = 0;
  rocksdb::Status s = partition->db()->ZAdd(key_, score_members, &count);
  if (s.ok()) {
    res_.AppendInteger(count);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}

void ZCardCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZCard);
    return;
  }
  key_ = argv_[1];
  return;
}

void ZCardCmd::Do(std::shared_ptr<Partition> partition) {
  int32_t card = 0;
  rocksdb::Status s = partition->db()->ZCard(key_, &card);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendInteger(card);
  } else {
    res_.SetRes(CmdRes::kErrOther, "zcard error");
  }
  return;
}

void ZScanCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZScan);
    return;
  }
  key_ = argv_[1];
  if (!pstd::string2int(argv_[2].data(), argv_[2].size(), &cursor_)) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZScan);
    return;
  }
  size_t argc = argv_.size(), index = 3;
  while (index < argc) {
    std::string opt = argv_[index];
    if (!strcasecmp(opt.data(), "match") || !strcasecmp(opt.data(), "count")) {
      index++;
      if (index >= argc) {
        res_.SetRes(CmdRes::kSyntaxErr);
        return;
      }
      if (!strcasecmp(opt.data(), "match")) {
        pattern_ = argv_[index];
      } else if (!pstd::string2int(argv_[index].data(), argv_[index].size(), &count_)) {
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
  return;
}

void ZScanCmd::Do(std::shared_ptr<Partition> partition) {
  int64_t next_cursor = 0;
  std::vector<storage::ScoreMember> score_members;
  rocksdb::Status s = partition->db()->ZScan(key_, cursor_, pattern_, count_, &score_members, &next_cursor);
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
  return;
}

void ZIncrbyCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZIncrby);
    return;
  }
  key_ = argv_[1];
  if (!pstd::string2d(argv_[2].data(), argv_[2].size(), &by_)) {
    res_.SetRes(CmdRes::kInvalidFloat);
    return;
  }
  member_ = argv_[3];
  return;
}

void ZIncrbyCmd::Do(std::shared_ptr<Partition> partition) {
  double score = 0;
  rocksdb::Status s = partition->db()->ZIncrby(key_, member_, by_, &score);
  if (s.ok()) {
    char buf[32];
    int64_t len = pstd::d2string(buf, sizeof(buf), score);
    res_.AppendStringLen(len);
    res_.AppendContent(buf);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}

void ZsetRangeParentCmd::DoInitial() {
  if (argv_.size() == 5 && !strcasecmp(argv_[4].data(), "withscores")) {
    is_ws_ = true;
  } else if (argv_.size() != 4) {
    res_.SetRes(CmdRes::kSyntaxErr);
    return;
  }
  key_ = argv_[1];
  if (!pstd::string2int(argv_[2].data(), argv_[2].size(), &start_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  if (!pstd::string2int(argv_[3].data(), argv_[3].size(), &stop_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  return;
}

void ZRangeCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZRange);
    return;
  }
  ZsetRangeParentCmd::DoInitial();
}

void ZRangeCmd::Do(std::shared_ptr<Partition> partition) {
  std::vector<storage::ScoreMember> score_members;
  rocksdb::Status s = partition->db()->ZRange(key_, start_, stop_, &score_members);
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
  return;
}

void ZRevrangeCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZRevrange);
    return;
  }
  ZsetRangeParentCmd::DoInitial();
}

void ZRevrangeCmd::Do(std::shared_ptr<Partition> partition) {
  std::vector<storage::ScoreMember> score_members;
  rocksdb::Status s = partition->db()->ZRevrange(key_, start_, stop_, &score_members);
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
  return;
}

int32_t DoScoreStrRange(std::string begin_score, std::string end_score, bool* left_close, bool* right_close,
                        double* min_score, double* max_score) {
  if (begin_score.size() > 0 && begin_score.at(0) == '(') {
    *left_close = false;
    begin_score.erase(begin_score.begin());
  }
  if (begin_score == "-inf") {
    *min_score = storage::ZSET_SCORE_MIN;
  } else if (begin_score == "inf" || begin_score == "+inf") {
    *min_score = storage::ZSET_SCORE_MAX;
  } else if (!pstd::string2d(begin_score.data(), begin_score.size(), min_score)) {
    return -1;
  }

  if (end_score.size() > 0 && end_score.at(0) == '(') {
    *right_close = false;
    end_score.erase(end_score.begin());
  }
  if (end_score == "+inf" || end_score == "inf") {
    *max_score = storage::ZSET_SCORE_MAX;
  } else if (end_score == "-inf") {
    *max_score = storage::ZSET_SCORE_MIN;
  } else if (!pstd::string2d(end_score.data(), end_score.size(), max_score)) {
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
    if (!strcasecmp(argv_[index].data(), "withscores")) {
      with_scores_ = true;
    } else if (!strcasecmp(argv_[index].data(), "limit")) {
      if (index + 3 > argc) {
        res_.SetRes(CmdRes::kSyntaxErr);
        return;
      }
      index++;
      if (!pstd::string2int(argv_[index].data(), argv_[index].size(), &offset_)) {
        res_.SetRes(CmdRes::kInvalidInt);
        return;
      }
      index++;
      if (!pstd::string2int(argv_[index].data(), argv_[index].size(), &count_)) {
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

void ZRangebyscoreCmd::Do(std::shared_ptr<Partition> partition) {
  if (min_score_ == storage::ZSET_SCORE_MAX || max_score_ == storage::ZSET_SCORE_MIN) {
    res_.AppendContent("*0");
    return;
  }
  std::vector<storage::ScoreMember> score_members;
  rocksdb::Status s =
      partition->db()->ZRangebyscore(key_, min_score_, max_score_, left_close_, right_close_, &score_members);
  if (!s.ok() && !s.IsNotFound()) {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;
  }
  FitLimit(count_, offset_, score_members.size());
  size_t index = offset_, end = offset_ + count_;
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
  return;
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

void ZRevrangebyscoreCmd::Do(std::shared_ptr<Partition> partition) {
  if (min_score_ == storage::ZSET_SCORE_MAX || max_score_ == storage::ZSET_SCORE_MIN) {
    res_.AppendContent("*0");
    return;
  }
  std::vector<storage::ScoreMember> score_members;
  rocksdb::Status s =
      partition->db()->ZRevrangebyscore(key_, min_score_, max_score_, left_close_, right_close_, &score_members);
  if (!s.ok() && !s.IsNotFound()) {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;
  }
  FitLimit(count_, offset_, score_members.size());
  int64_t index = offset_, end = offset_ + count_;
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
  return;
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
  return;
}

void ZCountCmd::Do(std::shared_ptr<Partition> partition) {
  if (min_score_ == storage::ZSET_SCORE_MAX || max_score_ == storage::ZSET_SCORE_MIN) {
    res_.AppendContent("*0");
    return;
  }

  int32_t count = 0;
  rocksdb::Status s = partition->db()->ZCount(key_, min_score_, max_score_, left_close_, right_close_, &count);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendInteger(count);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}

void ZRemCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZRem);
    return;
  }
  key_ = argv_[1];
  PikaCmdArgsType::iterator iter = argv_.begin() + 2;
  members_.assign(iter, argv_.end());
  return;
}

void ZRemCmd::Do(std::shared_ptr<Partition> partition) {
  int32_t count = 0;
  rocksdb::Status s = partition->db()->ZRem(key_, members_, &count);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendInteger(count);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}

void ZsetUIstoreParentCmd::DoInitial() {
  dest_key_ = argv_[1];
  if (!pstd::string2int(argv_[2].data(), argv_[2].size(), &num_keys_)) {
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
    if (!strcasecmp(argv_[index].data(), "weights")) {
      index++;
      if (argc < index + num_keys_) {
        res_.SetRes(CmdRes::kSyntaxErr);
        return;
      }
      double weight;
      int base = index;
      for (; index < base + num_keys_; index++) {
        if (!pstd::string2d(argv_[index].data(), argv_[index].size(), &weight)) {
          res_.SetRes(CmdRes::kErrOther, "weight value is not a float");
          return;
        }
        weights_[index - base] = weight;
      }
    } else if (!strcasecmp(argv_[index].data(), "aggregate")) {
      index++;
      if (argc < index + 1) {
        res_.SetRes(CmdRes::kSyntaxErr);
        return;
      }
      if (!strcasecmp(argv_[index].data(), "sum")) {
        aggregate_ = storage::SUM;
      } else if (!strcasecmp(argv_[index].data(), "min")) {
        aggregate_ = storage::MIN;
      } else if (!strcasecmp(argv_[index].data(), "max")) {
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
  return;
}

void ZUnionstoreCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZUnionstore);
    return;
  }
  ZsetUIstoreParentCmd::DoInitial();
}

void ZUnionstoreCmd::Do(std::shared_ptr<Partition> partition) {
  int32_t count = 0;
  rocksdb::Status s = partition->db()->ZUnionstore(dest_key_, keys_, weights_, aggregate_, &count);
  if (s.ok()) {
    res_.AppendInteger(count);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}

void ZInterstoreCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZInterstore);
    return;
  }
  ZsetUIstoreParentCmd::DoInitial();
  return;
}

void ZInterstoreCmd::Do(std::shared_ptr<Partition> partition) {
  int32_t count = 0;
  rocksdb::Status s = partition->db()->ZInterstore(dest_key_, keys_, weights_, aggregate_, &count);
  if (s.ok()) {
    res_.AppendInteger(count);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}

void ZsetRankParentCmd::DoInitial() {
  key_ = argv_[1];
  member_ = argv_[2];
  return;
}

void ZRankCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZRank);
    return;
  }
  ZsetRankParentCmd::DoInitial();
}

void ZRankCmd::Do(std::shared_ptr<Partition> partition) {
  int32_t rank = 0;
  rocksdb::Status s = partition->db()->ZRank(key_, member_, &rank);
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

void ZRevrankCmd::Do(std::shared_ptr<Partition> partition) {
  int32_t revrank = 0;
  rocksdb::Status s = partition->db()->ZRevrank(key_, member_, &revrank);
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

void ZScoreCmd::Do(std::shared_ptr<Partition> partition) {
  double score = 0;
  rocksdb::Status s = partition->db()->ZScore(key_, member_, &score);
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
  return;
}

static int32_t DoMemberRange(const std::string& raw_min_member, const std::string& raw_max_member, bool* left_close,
                             bool* right_close, std::string* min_member, std::string* max_member) {
  if (raw_min_member == "-") {
    *min_member = "-";
  } else if (raw_min_member == "+") {
    *min_member = "+";
  } else {
    if (raw_min_member.size() > 0 && raw_min_member.at(0) == '(') {
      *left_close = false;
    } else if (raw_min_member.size() > 0 && raw_min_member.at(0) == '[') {
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
    if (raw_max_member.size() > 0 && raw_max_member.at(0) == '(') {
      *right_close = false;
    } else if (raw_max_member.size() > 0 && raw_max_member.at(0) == '[') {
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
  } else if (argc != 7 || strcasecmp(argv_[4].data(), "limit")) {
    res_.SetRes(CmdRes::kSyntaxErr);
    return;
  }
  if (!pstd::string2int(argv_[5].data(), argv_[5].size(), &offset_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  if (!pstd::string2int(argv_[6].data(), argv_[6].size(), &count_)) {
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

void ZRangebylexCmd::Do(std::shared_ptr<Partition> partition) {
  if (min_member_ == "+" || max_member_ == "-") {
    res_.AppendContent("*0");
    return;
  }
  std::vector<std::string> members;
  rocksdb::Status s = partition->db()->ZRangebylex(key_, min_member_, max_member_, left_close_, right_close_, &members);
  if (!s.ok() && !s.IsNotFound()) {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;
  }
  FitLimit(count_, offset_, members.size());

  res_.AppendArrayLen(count_);
  size_t index = offset_, end = offset_ + count_;
  for (; index < end; index++) {
    res_.AppendStringLen(members[index].size());
    res_.AppendContent(members[index]);
  }
  return;
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

void ZRevrangebylexCmd::Do(std::shared_ptr<Partition> partition) {
  if (min_member_ == "+" || max_member_ == "-") {
    res_.AppendContent("*0");
    return;
  }
  std::vector<std::string> members;
  rocksdb::Status s = partition->db()->ZRangebylex(key_, min_member_, max_member_, left_close_, right_close_, &members);
  if (!s.ok() && !s.IsNotFound()) {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;
  }
  FitLimit(count_, offset_, members.size());

  res_.AppendArrayLen(count_);
  int64_t index = members.size() - 1 - offset_, end = index - count_;
  for (; index > end; index--) {
    res_.AppendStringLen(members[index].size());
    res_.AppendContent(members[index]);
  }
  return;
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

void ZLexcountCmd::Do(std::shared_ptr<Partition> partition) {
  if (min_member_ == "+" || max_member_ == "-") {
    res_.AppendContent(":0");
    return;
  }
  int32_t count = 0;
  rocksdb::Status s = partition->db()->ZLexcount(key_, min_member_, max_member_, left_close_, right_close_, &count);
  if (!s.ok() && !s.IsNotFound()) {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;
  }
  res_.AppendInteger(count);
  return;
}

void ZRemrangebyrankCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZRemrangebyrank);
    return;
  }
  key_ = argv_[1];
  if (!pstd::string2int(argv_[2].data(), argv_[2].size(), &start_rank_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  if (!pstd::string2int(argv_[3].data(), argv_[3].size(), &stop_rank_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
}

void ZRemrangebyrankCmd::Do(std::shared_ptr<Partition> partition) {
  int32_t count = 0;
  rocksdb::Status s = partition->db()->ZRemrangebyrank(key_, start_rank_, stop_rank_, &count);
  if (s.ok() || s.IsNotFound()) {
    res_.AppendInteger(count);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
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
  return;
}

void ZRemrangebyscoreCmd::Do(std::shared_ptr<Partition> partition) {
  if (min_score_ == storage::ZSET_SCORE_MAX || max_score_ == storage::ZSET_SCORE_MIN) {
    res_.AppendContent(":0");
    return;
  }
  int32_t count = 0;
  rocksdb::Status s =
      partition->db()->ZRemrangebyscore(key_, min_score_, max_score_, left_close_, right_close_, &count);
  if (!s.ok() && !s.IsNotFound()) {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;
  }
  res_.AppendInteger(count);
  return;
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
  return;
}

void ZRemrangebylexCmd::Do(std::shared_ptr<Partition> partition) {
  if (min_member_ == "+" || max_member_ == "-") {
    res_.AppendContent("*0");
    return;
  }
  int32_t count = 0;
  rocksdb::Status s =
      partition->db()->ZRemrangebylex(key_, min_member_, max_member_, left_close_, right_close_, &count);
  if (!s.ok() && !s.IsNotFound()) {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;
  }
  res_.AppendInteger(count);
  return;
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
  if (!pstd::string2int(argv_[2].data(), argv_[2].size(), (long long*)(&count_))) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
}

void ZPopmaxCmd::Do(std::shared_ptr<Partition> partition) {
  std::vector<storage::ScoreMember> score_members;
  rocksdb::Status s = partition->db()->ZPopMax(key_, count_, &score_members);
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
  if (!pstd::string2int(argv_[2].data(), argv_[2].size(), (long long*)(&count_))) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
}

void ZPopminCmd::Do(std::shared_ptr<Partition> partition) {
  std::vector<storage::ScoreMember> score_members;
  rocksdb::Status s = partition->db()->ZPopMin(key_, count_, &score_members);
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
