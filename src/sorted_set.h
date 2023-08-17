/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <map>
#include <set>
#include <unordered_map>
#include <vector>
#include "helper.h"
#include "pstring.h"

namespace pikiwidb {

class PSortedSet {
 public:
  using Members = std::set<PString>;
  using Score2Members = std::map<double, Members>;

  using Member2Score = std::unordered_map<PString, double, my_hash, std::equal_to<PString> >;

  Member2Score::iterator FindMember(const PString& member);
  Member2Score::const_iterator begin() const { return members_.begin(); };
  Member2Score::iterator begin() { return members_.begin(); };
  Member2Score::const_iterator end() const { return members_.end(); };
  Member2Score::iterator end() { return members_.end(); };
  void AddMember(const PString& member, double score);
  double UpdateMember(const Member2Score::iterator& itMem, double delta);

  int Rank(const PString& member) const;     // 0-based
  int RevRank(const PString& member) const;  // 0-based
  bool DelMember(const PString& member);
  Member2Score::value_type GetMemberByRank(std::size_t rank) const;

  std::vector<Member2Score::value_type> RangeByRank(long start, long end) const;

  std::vector<Member2Score::value_type> RangeByScore(double minScore, double maxScore);
  std::size_t Size() const;

 private:
  Score2Members scores_;
  Member2Score members_;
};

}  // namespace pikiwidb

