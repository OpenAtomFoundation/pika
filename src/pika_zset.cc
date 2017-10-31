// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "slash/include/slash_string.h"
#include "nemo.h"
#include "pika_zset.h"
#include "pika_server.h"
#include "pika_slot.h"

extern PikaServer *g_pika_server;

void ZAddCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZAdd);
    return;
  }
  size_t argc = argv.size();
  if (argc % 2 == 1) {
    res_.SetRes(CmdRes::kSyntaxErr);
    return;
  }
  key_ = argv[1];
  sms_v_.clear();
  double score;
  size_t index = 2;
  for (; index < argc; index += 2) {
    if (!slash::string2d(argv[index].data(), argv[index].size(), &score)) {
      res_.SetRes(CmdRes::kInvalidFloat);
      return;
    }
    sms_v_.push_back({score, argv[index+1]});
  }
  return;
}

void ZAddCmd::Do() {
  nemo::Status s;
  int64_t count = 0, ret;
  const std::shared_ptr<nemo::Nemo> db = g_pika_server->db();
  std::vector<nemo::SM>::const_iterator iter = sms_v_.begin();
  for (; iter != sms_v_.end(); iter++) {
    s = db->ZAdd(key_, iter->score, iter->member, &ret);
    if (s.ok()) {
      count += ret;
    } else {
      res_.SetRes(CmdRes::kErrOther, s.ToString());
      return;
    }
  }
  SlotKeyAdd("z", key_);
  res_.AppendInteger(count);
  return;
}

void ZCardCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZCard);
    return;
  }
  key_ = argv[1];
  return;
}

void ZCardCmd::Do() {
  int64_t card = g_pika_server->db()->ZCard(key_);
  if (card >= 0) {
    res_.AppendInteger(card);
  } else {
    res_.SetRes(CmdRes::kErrOther, "zcard error");
  }
  return;
}

void ZScanCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZScan);
    return;
  }
  key_ = argv[1];
  if (!slash::string2l(argv[2].data(), argv[2].size(), &cursor_)) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZScan);
    return;
  }
  size_t argc = argv.size(), index = 3;
  while (index < argc) {
    std::string opt = slash::StringToLower(argv[index]); 
    if (opt == "match" || opt == "count") {
      index++;
      if (index >= argc) {
        res_.SetRes(CmdRes::kSyntaxErr);
        return;
      }
      if (opt == "match") {
        pattern_ = argv[index];
      } else if (!slash::string2l(argv[index].data(), argv[index].size(), &count_)) {
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

void ZScanCmd::Do() {
  int64_t card = g_pika_server->db()->ZCard(key_);
  if (card >= 0 && cursor_ >= card) {
    cursor_ = 0;
  }
  if (card <= 128) {
    count_ = 128;
  }
  std::vector<nemo::SM> sms_v;
  nemo::ZIterator *iter = g_pika_server->db()->ZScan(key_, nemo::ZSET_SCORE_MIN, nemo::ZSET_SCORE_MAX, -1);
  iter->Skip(cursor_);
  if (!iter->Valid()) {
    delete iter;
    iter = g_pika_server->db()->ZScan(key_, nemo::ZSET_SCORE_MIN, nemo::ZSET_SCORE_MAX, -1);
    cursor_ = 0;
  }
  for (; iter->Valid() && count_; iter->Next()) {
    count_--;
    cursor_++;
    if (pattern_ != "*" && !slash::stringmatchlen(pattern_.data(), pattern_.size(), iter->member().data(), iter->member().size(), 0)) {
      continue;
    }
    sms_v.push_back({iter->score(), iter->member()});
  }
  if (!iter->Valid()) {
    cursor_ = 0;
  }
  res_.AppendContent("*2");
  
  char buf[32];
  int64_t len = slash::ll2string(buf, sizeof(buf), cursor_);
  res_.AppendStringLen(len);
  res_.AppendContent(buf);

  res_.AppendArrayLen(sms_v.size() * 2); 
  std::vector<nemo::SM>::const_iterator iter_sm = sms_v.begin();
  for (; iter_sm != sms_v.end(); iter_sm++) {
    res_.AppendStringLen(iter_sm->member.size());
    res_.AppendContent(iter_sm->member);
    len = slash::d2string(buf, sizeof(buf), iter_sm->score);
    res_.AppendStringLen(len);
    res_.AppendContent(buf);
  }
  delete iter;
  return;
}

void ZIncrbyCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZIncrby);
    return;
  }
  key_ = argv[1];
  if (!slash::string2d(argv[2].data(), argv[2].size(), &by_)) {
    res_.SetRes(CmdRes::kInvalidFloat);
    return;
  }
  member_ = argv[3];
  return;
}

void ZIncrbyCmd::Do() {
  std::string new_value;
  nemo::Status s = g_pika_server->db()->ZIncrby(key_, member_, by_, new_value);
  if (s.ok()) {
    res_.AppendStringLen(new_value.size());
    res_.AppendContent(new_value);
    SlotKeyAdd("z", key_);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}

void ZsetRangeParentCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  (void)ptr_info;
  if (argv.size() == 5 && slash::StringToLower(argv[4]) == "withscores") {
    is_ws_ = true;
  } else if (argv.size() != 4) {
    res_.SetRes(CmdRes::kSyntaxErr);
    return;
  }
  key_ = argv[1];
  if (!slash::string2l(argv[2].data(), argv[2].size(), &start_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  if (!slash::string2l(argv[3].data(), argv[3].size(), &stop_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  return;
}

void ZRangeCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZRange);
    return;
  }
  ZsetRangeParentCmd::DoInitial(argv, NULL);
}

void ZRangeCmd::Do() {
  std::vector<nemo::SM> sms_v;
  nemo::Status s = g_pika_server->db()->ZRange(key_, start_, stop_, sms_v);
  if (s.ok()) {
    std::vector<nemo::SM>::const_iterator iter = sms_v.begin();
    if (is_ws_) {
      res_.AppendArrayLen(sms_v.size()*2);
      char buf[32];
      int64_t len;
      for (; iter != sms_v.end(); iter++) {
        res_.AppendStringLen(iter->member.size());
        res_.AppendContent(iter->member);
        len = slash::d2string(buf, sizeof(buf), iter->score);
        res_.AppendStringLen(len);
        res_.AppendContent(buf);
      }
    } else {
      res_.AppendArrayLen(sms_v.size());
      for (; iter != sms_v.end(); iter++) {
        res_.AppendStringLen(iter->member.size());
        res_.AppendContent(iter->member);
      }
    }
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}

static void Calcmirror(int64_t &start, int64_t &stop, int64_t t_size) {
    int64_t t_start = stop >= 0 ? stop : t_size + stop;
    int64_t t_stop = start >= 0 ? start : t_size + start;
    t_start = t_size - 1 - t_start;
    t_stop = t_size - 1 - t_stop;
    start = t_start >= 0 ? t_start : t_start - t_size;
    stop = t_stop >= 0 ? t_stop : t_stop - t_size;
}

void ZRevrangeCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZRevrange);
    return;
  }
  ZsetRangeParentCmd::DoInitial(argv, NULL);
  int64_t card = g_pika_server->db()->ZCard(key_);
  if (card < 0) {
    res_.SetRes(CmdRes::kErrOther, "zcard error");
    return;
  }
  Calcmirror(start_, stop_, card);
}

void ZRevrangeCmd::Do() {
  std::vector<nemo::SM> sms_v;
  nemo::Status s = g_pika_server->db()->ZRange(key_, start_, stop_, sms_v);
  if (s.ok()) {
    std::vector<nemo::SM>::const_reverse_iterator iter = sms_v.rbegin();
    if (is_ws_) {
      res_.AppendArrayLen(sms_v.size()*2);
      char buf[32];
      int64_t len;
      for (; iter != sms_v.rend(); iter++) {
        res_.AppendStringLen(iter->member.size());
        res_.AppendContent(iter->member);
        len = slash::d2string(buf, sizeof(buf), iter->score);
        res_.AppendStringLen(len);
        res_.AppendContent(buf);
      }
    } else {
      res_.AppendArrayLen(sms_v.size());
      for (; iter != sms_v.rend(); iter++) {
        res_.AppendStringLen(iter->member.size());
        res_.AppendContent(iter->member);
      }
    }
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}

static nemo::Status DoScoreStrRange(std::string begin_score, std::string end_score, bool *is_lo, bool *is_ro, double *min_score, double *max_score) {
  if (begin_score.size() > 0 && begin_score.at(0) == '(') {
    *is_lo = true;
    begin_score.erase(begin_score.begin());
  }
  if (begin_score == "-inf") {
    *min_score = nemo::ZSET_SCORE_MIN;
  } else if (begin_score == "inf" || begin_score == "+inf") {
    *min_score = nemo::ZSET_SCORE_MAX;
  } else if (!slash::string2d(begin_score.data(), begin_score.size(), min_score)) {
    return nemo::Status::Corruption("min or max is not a float");
  } 
  
  if (end_score.size() > 0 && end_score.at(0) == '(') {
    *is_ro = true;
    end_score.erase(end_score.begin());
  }
  if (end_score == "+inf" || end_score == "inf") {
    *max_score = nemo::ZSET_SCORE_MAX; 
  } else if (end_score == "-inf") {
    *max_score = nemo::ZSET_SCORE_MIN;
  } else if (!slash::string2d(end_score.data(), end_score.size(), max_score)) {
    return nemo::Status::Corruption("min or max is not a float");
  }
  return nemo::Status();
}

static void FitLimit(int64_t &count, int64_t &offset, const int64_t size) {
  count = count >= 0 ? count : size;
  offset = (offset >= 0 && offset < size) ? offset : size;
  count = (offset + count < size) ? count : size - offset;
}

void ZsetRangebyscoreParentCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  (void)ptr_info;
  key_ = argv[1];
  nemo::Status s = DoScoreStrRange(argv[2], argv[3], &is_lo_, &is_ro_, &min_score_, &max_score_);
  if (!s.ok()) {
    res_.SetRes(CmdRes::kErrOther, "min or max is not a float");
    return;
  }
  size_t argc = argv.size();
  if (argc < 5) {
    return;
  }
  size_t index = 4;
  while (index < argc) {
    slash::StringToLower(argv[index]);
    if (argv[index] == "withscores") {
      is_ws_ = true;
    } else if (argv[index] == "limit") {
      if (index + 3 > argc) {
        res_.SetRes(CmdRes::kSyntaxErr);
        return;
      }
      index++;
      if (!slash::string2l(argv[index].data(), argv[index].size(), &offset_)) {
        res_.SetRes(CmdRes::kInvalidInt);
        return;
      }
      index++;
      if (!slash::string2l(argv[index].data(), argv[index].size(), &count_)) {
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

void ZRangebyscoreCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZRangebyscore);
    return;
  }
  ZsetRangebyscoreParentCmd::DoInitial(argv, NULL);
}

void ZRangebyscoreCmd::Do() {
  if (min_score_ == nemo::ZSET_SCORE_MAX || max_score_ == nemo::ZSET_SCORE_MIN) {
    res_.AppendContent("*0");
    return;
  }
  std::vector<nemo::SM> sms_v;
  nemo::Status s = g_pika_server->db()->ZRangebyscore(key_, min_score_, max_score_, sms_v, is_lo_, is_ro_);
  if (!s.ok()) {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;
  }
  FitLimit(count_, offset_, sms_v.size());
  size_t index = offset_, end = offset_ + count_;
  if (is_ws_) {
    res_.AppendArrayLen(count_ * 2);
    char buf[32];
    int64_t len;
    for (; index < end; index++) {
      res_.AppendStringLen(sms_v[index].member.size());
      res_.AppendContent(sms_v[index].member);
      len = slash::d2string(buf, sizeof(buf), sms_v[index].score);
      res_.AppendStringLen(len);
      res_.AppendContent(buf);
    }
  } else {
    res_.AppendArrayLen(count_);
    for (; index < end; index++) {
      res_.AppendStringLen(sms_v[index].member.size());
      res_.AppendContent(sms_v[index].member);
    }
  }
  return;
}

void ZRevrangebyscoreCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZRevrangebyscore);
    return;
  }
  ZsetRangebyscoreParentCmd::DoInitial(argv, NULL);
  double tmp_d;
  tmp_d = min_score_;
  min_score_ = max_score_;
  max_score_ = tmp_d;

  bool tmp_b;
  tmp_b = is_lo_;
  is_lo_ = is_ro_;
  is_ro_ = tmp_b;
}

void ZRevrangebyscoreCmd::Do() {
  if (min_score_ == nemo::ZSET_SCORE_MAX || max_score_ == nemo::ZSET_SCORE_MIN) {
    res_.AppendContent("*0");
    return;
  }
  std::vector<nemo::SM> sms_v;
  nemo::Status s = g_pika_server->db()->ZRangebyscore(key_, min_score_, max_score_, sms_v, is_lo_, is_ro_);
  if (!s.ok()) {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;
  }
  FitLimit(count_, offset_, sms_v.size());
  int64_t index = sms_v.size() - 1 - offset_, end = index - count_;
  if (is_ws_) {
    res_.AppendArrayLen(count_ * 2);
    char buf[32];
    int64_t len;
    for (; index > end; index--) {
      res_.AppendStringLen(sms_v[index].member.size());
      res_.AppendContent(sms_v[index].member);
      len = slash::d2string(buf, sizeof(buf), sms_v[index].score);
      res_.AppendStringLen(len);
      res_.AppendContent(buf);
    }
  } else {
    res_.AppendArrayLen(count_);
    for (; index > end; index--) {
      res_.AppendStringLen(sms_v[index].member.size());
      res_.AppendContent(sms_v[index].member);
    }
  }
  return;
}

void ZCountCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZCount);
    return;
  }
  key_ = argv[1];
  nemo::Status s = DoScoreStrRange(argv[2], argv[3], &is_lo_, &is_ro_, &min_score_, &max_score_);
  if (!s.ok()) {
    res_.SetRes(CmdRes::kErrOther, "min or max is not a float");
    return;
  }
  return;
}

void ZCountCmd::Do() {
  if (min_score_ == nemo::ZSET_SCORE_MAX || max_score_ == nemo::ZSET_SCORE_MIN) {
    res_.AppendContent("*0");
    return;
  }
  int64_t count = g_pika_server->db()->ZCount(key_, min_score_, max_score_, is_lo_, is_ro_);
  res_.AppendInteger(count);
}

void ZRemCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZRem);
    return;
  }
  key_ = argv[1];
  PikaCmdArgsType::iterator iter = argv.begin() + 2;
  members_.assign(iter, argv.end());
  return;
}

void ZRemCmd::Do() {
  int64_t count = 0, tmp;
  std::vector<std::string>::const_iterator iter = members_.begin();
  nemo::Status s;
  std::shared_ptr<nemo::Nemo> db = g_pika_server->db();
  for (; iter != members_.end(); iter++) {
    s = db->ZRem(key_, *iter, &tmp);
    if (s.ok()) {
      count++;
    }
  }
  res_.AppendInteger(count);

  KeyNotExistsRem("z", key_);
  return;
}

void ZsetUIstoreParentCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  (void)ptr_info;
  dest_key_ = argv[1];
  if (!slash::string2l(argv[2].data(), argv[2].size(), &num_keys_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  if (num_keys_ < 1) {
    res_.SetRes(CmdRes::kErrOther, "at least 1 input key is needed for ZUNIONSTORE/ZINTERSTORE");
    return;
  }
  int argc = argv.size();
  if (argc < num_keys_ + 3) {
    res_.SetRes(CmdRes::kSyntaxErr);
    return;
  }
  keys_.assign(argv.begin() + 3, argv.begin() + 3 + num_keys_);
  weights_.assign(num_keys_, 1);
  int index = num_keys_ + 3;
  while (index < argc) {
    slash::StringToLower(argv[index]);
    if (argv[index] == "weights") {
      index++;
      if (argc < index + num_keys_) {
        res_.SetRes(CmdRes::kSyntaxErr);
        return;
      }
      double weight;
      int base = index;
      for (; index < base + num_keys_; index++) {
        if (!slash::string2d(argv[index].data(), argv[index].size(), &weight)) {
          res_.SetRes(CmdRes::kErrOther, "weight value is not a float");
          return;
        }
        weights_[index-base] = weight;
      }
    } else if (argv[index] == "aggregate") {
      index++;
      if (argc < index + 1) {
        res_.SetRes(CmdRes::kSyntaxErr);
        return;
      }
      slash::StringToLower(argv[index]);
      if (argv[index] == "sum") {
        aggregate_ = nemo::SUM;
      } else if (argv[index] == "min") {
        aggregate_ = nemo::MIN;
      } else if (argv[index] == "max") {
        aggregate_ = nemo::MAX;
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

void ZUnionstoreCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZUnionstore);
    return;
  }
  ZsetUIstoreParentCmd::DoInitial(argv, NULL);
}

void ZUnionstoreCmd::Do() {
  int64_t count = 0;
  nemo::Status s = g_pika_server->db()->ZUnionStore(dest_key_, num_keys_, keys_, weights_, aggregate_, &count);
  if (s.ok()) {
    res_.AppendInteger(count);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}

void ZInterstoreCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZInterstore);
    return;
  }
  ZsetUIstoreParentCmd::DoInitial(argv, NULL);
  return;
}

void ZInterstoreCmd::Do() {
  int64_t count = 0;
  nemo::Status s = g_pika_server->db()->ZInterStore(dest_key_, num_keys_, keys_, weights_, aggregate_, &count);
  if (s.ok()) {
    res_.AppendInteger(count);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}

void ZsetRankParentCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  (void)ptr_info;
  key_ = argv[1];
  member_ = argv[2];
  return;
}

void ZRankCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZRank);
    return;
  }
  ZsetRankParentCmd::DoInitial(argv, NULL);
}

void ZRankCmd::Do() {
  int64_t rank = 0;
  nemo::Status s = g_pika_server->db()->ZRank(key_, member_, &rank);
  if (s.ok()) {
    res_.AppendInteger(rank);
  } else if (s.IsNotFound()){
    res_.AppendContent("$-1");
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void ZRevrankCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZRevrank);
    return;
  }
  ZsetRankParentCmd::DoInitial(argv, NULL);
}

void ZRevrankCmd::Do() {
  int64_t revrank = 0;
  nemo::Status s = g_pika_server->db()->ZRevrank(key_, member_, &revrank);
  if (s.ok()) {
    res_.AppendInteger(revrank);
  } else if (s.IsNotFound()){
    res_.AppendContent("$-1");
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void ZScoreCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZScore);
    return;
  }
  key_ = argv[1];
  member_ = argv[2];
}

void ZScoreCmd::Do() {
  double score;
  nemo::Status s = g_pika_server->db()->ZScore(key_, member_, &score);
  if (s.ok()) {
    char buf[32];
    int64_t len = slash::d2string(buf, sizeof(buf), score);
    res_.AppendStringLen(len);
    res_.AppendContent(buf);
  } else if (s.IsNotFound()) {
    res_.AppendContent("$-1");
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());  }
  return;
}

static nemo::Status DoMemberRange(const std::string &raw_min_member, const std::string &raw_max_member, bool *is_lo, bool *is_ro, std::string &min_member, std::string &max_member) {
  if (raw_min_member == "-") {
    min_member = "-";
  } else if (raw_min_member == "+") {
    min_member = "+";
  } else {
    if (raw_min_member.size() > 0 && raw_min_member.at(0) == '(') {
      *is_lo = true;
    } else if (raw_min_member.size() > 0 && raw_min_member.at(0) == '[') {
      *is_lo = false;
    } else {
      return nemo::Status::Corruption("min or max not valid string range item");
    }
    min_member.assign(raw_min_member.begin() + 1, raw_min_member.end());
  }

  if (raw_max_member == "+") {
    max_member = "+";
  } else if (raw_max_member == "-") {
    max_member = "-";
  } else {
    if (raw_max_member.size() > 0 && raw_max_member.at(0) == '(') {
      *is_ro = true;
    } else if (raw_max_member.size() > 0 && raw_max_member.at(0) == '[') {
      *is_ro = false;
    } else {
      return nemo::Status::Corruption("min or max not valid string range item");
    }
    max_member.assign(raw_max_member.begin() + 1, raw_max_member.end());
  }
  return nemo::Status();
}

void ZsetRangebylexParentCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  (void)ptr_info;
  key_ = argv[1];
  nemo::Status s = DoMemberRange(argv[2], argv[3], &is_lo_, &is_ro_, min_member_, max_member_);
  if (!s.ok()) {
    res_.SetRes(CmdRes::kErrOther, "min or max not valid string range item");
    return;
  }
  size_t argc = argv.size();
  if (argc == 4) {
    return;
  } else if (argc != 7 || slash::StringToLower(argv[4]) != "limit") {
    res_.SetRes(CmdRes::kSyntaxErr);
    return;
  }
  if (!slash::string2l(argv[5].data(), argv[5].size(), &offset_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  if (!slash::string2l(argv[6].data(), argv[6].size(), &count_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
}

void ZRangebylexCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZRangebylex);
    return;
  }
  ZsetRangebylexParentCmd::DoInitial(argv, NULL); 
  if (min_member_ == "-") {
    min_member_ = "";
  }
  if (max_member_ == "+") {
    max_member_ = "";
  }
}

void ZRangebylexCmd::Do() {
  if (min_member_ == "+" || max_member_ == "-") {
    res_.AppendContent("*0");
    return;
  }
  std::vector<std::string> members;
  nemo::Status s = g_pika_server->db()->ZRangebylex(key_, min_member_, max_member_, members);
  if (!s.ok()) {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;
  }
  if (is_lo_ && members.size() > 0 && members.front().compare(min_member_) == 0) {
    members.erase(members.begin());
  }
  if (is_ro_ && members.size() > 0 && members.back().compare(max_member_) == 0) {
    members.pop_back();
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

void ZRevrangebylexCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZRevrangebylex);
    return;
  }
  ZsetRangebylexParentCmd::DoInitial(argv, NULL); 

  std::string tmp_s;
  tmp_s = min_member_;
  min_member_ = max_member_;
  max_member_ = tmp_s;

  if (min_member_ == "-") {
    min_member_ = "";
  }
  if (max_member_ == "+") {
    max_member_ = "";
  }

  bool tmp_b;
  tmp_b = is_lo_;
  is_lo_ = is_ro_;
  is_ro_ = tmp_b;
}

void ZRevrangebylexCmd::Do() {
  if (min_member_ == "+" || max_member_ == "-") {
    res_.AppendContent("*0");
    return;
  }
  std::vector<std::string> members;
  nemo::Status s = g_pika_server->db()->ZRangebylex(key_, min_member_, max_member_, members);
  if (!s.ok()) {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;
  }
  if (is_lo_ && members.size() > 0 && members.front().compare(min_member_) == 0) {
    members.erase(members.begin());
  }
  if (is_ro_ && members.size() > 0 && members.back().compare(max_member_) == 0) {
    members.pop_back();
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

void ZLexcountCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZLexcount);
    return;
  }
  key_ = argv[1];
  nemo::Status s = DoMemberRange(argv[2], argv[3], &is_lo_, &is_ro_, min_member_, max_member_);
  if (!s.ok()) {
    res_.SetRes(CmdRes::kErrOther, "min or max not valid string range item");
    return;
  }
  if (min_member_ == "-") {
    min_member_ = "";
  }
  if (max_member_ == "+") {
    max_member_ = "";
  }
}

void ZLexcountCmd::Do() {
  if (min_member_ == "+" || max_member_ == "-") {
    res_.AppendContent(":0");
    return;
  }
  std::vector<std::string> members;
  nemo::Status s = g_pika_server->db()->ZRangebylex(key_, min_member_, max_member_, members);
  if (!s.ok()) {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;
  }
  if (is_lo_ && members.size() > 0 && members.front().compare(min_member_) == 0) {
    members.erase(members.begin());
  }
  if (is_ro_ && members.size() > 0 && members.back().compare(max_member_) == 0) {
    members.pop_back();
  }
  res_.AppendInteger(members.size());
  return;
}

void ZRemrangebyrankCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZRemrangebyrank);
    return;
  }
  key_ = argv[1];
  if (!slash::string2l(argv[2].data(), argv[2].size(), &start_rank_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  if (!slash::string2l(argv[3].data(), argv[3].size(), &stop_rank_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
}

void ZRemrangebyrankCmd::Do() {
  int64_t count;
  nemo::Status s = g_pika_server->db()->ZRemrangebyrank(key_, start_rank_, stop_rank_, &count);
  if (s.ok()) {
    res_.AppendInteger(count);
    KeyNotExistsRem("z", key_);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}

void ZRemrangebyscoreCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZRemrangebyscore);
    return;
  }
  key_ = argv[1];
  nemo::Status s = DoScoreStrRange(argv[2], argv[3], &is_lo_, &is_ro_, &min_score_, &max_score_);
  if (!s.ok()) {
    res_.SetRes(CmdRes::kErrOther, "min or max is not a float");
    return;
  }
  return;
}

void ZRemrangebyscoreCmd::Do() {
  if (min_score_ == nemo::ZSET_SCORE_MAX || max_score_ == nemo::ZSET_SCORE_MIN) {
    res_.AppendContent(":0");
    return;
  }
  int64_t count;
  nemo::Status s = g_pika_server->db()->ZRemrangebyscore(key_, min_score_, max_score_, &count, is_lo_, is_ro_);
  if (!s.ok()) {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;
  }
  KeyNotExistsRem("z", key_);
  res_.AppendInteger(count);
  return;
}

void ZRemrangebylexCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZRemrangebylex);
    return;
  }
  key_ = argv[1];
  nemo::Status s = DoMemberRange(argv[2], argv[3], &is_lo_, &is_ro_, min_member_, max_member_);
  if (!s.ok()) {
    res_.SetRes(CmdRes::kErrOther, "min or max not valid string range item");
    return;
  }
  if (min_member_ == "-") {
    min_member_ = "";
  }
  if (max_member_ == "+") {
    max_member_ = "";
  }
  return;
}

void ZRemrangebylexCmd::Do() {
  if (min_member_ == "+" || max_member_ == "-") {
    res_.AppendContent("*0");
    return;
  }
  int64_t count;
  nemo::Status s = g_pika_server->db()->ZRemrangebylex(key_, min_member_, max_member_, is_lo_, is_ro_, &count);
  if (!s.ok()) {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;
  }
  KeyNotExistsRem("z", key_);
  res_.AppendInteger(count);
  return;
}
