/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "sorted_set.h"
#include <cassert>
#include "log.h"
#include "store.h"

namespace pikiwidb {

PSortedSet::Member2Score::iterator PSortedSet::FindMember(const PString& member) { return members_.find(member); }

void PSortedSet::AddMember(const PString& member, double score) {
  assert(FindMember(member) == members_.end());

  members_.insert(Member2Score::value_type(member, score));
  scores_[score].insert(member);
}

double PSortedSet::UpdateMember(const Member2Score::iterator& itMem, double delta) {
  auto oldScore = itMem->second;
  auto newScore = oldScore + delta;
  itMem->second = newScore;

  auto itScore(scores_.find(oldScore));
  assert(itScore != scores_.end());

  size_t ret = itScore->second.erase(itMem->first);
  assert(ret == 1);

  bool succ = scores_[newScore].insert(itMem->first).second;
  assert(succ);

  return newScore;
}

int PSortedSet::Rank(const PString& member) const {
  double score;
  auto itMem(members_.find(member));
  if (itMem != members_.end()) {
    score = itMem->second;
  } else {
    return -1;
  }

  int rank = 0;
  for (auto it(scores_.begin()); it != scores_.end(); rank += it->second.size(), ++it) {
    if (it->first == score) {
      auto iter(it->second.begin());
      for (; iter != it->second.end(); ++iter, ++rank) {
        if (*iter == member) {
          return rank;
        }
      }

      assert(!!!"Why can not find member");
    }
  }

  assert(!!!"Why can not find score");
  return -1;
}

int PSortedSet::RevRank(const PString& member) const {
  int rank = Rank(member);
  if (rank == -1) {
    return rank;
  }

  return static_cast<int>(members_.size() - (rank + 1));
}

bool PSortedSet::DelMember(const PString& member) {
  double score = 0;
  Member2Score::const_iterator itMem(members_.find(member));
  if (itMem != members_.end()) {
    score = itMem->second;
    members_.erase(itMem);
  } else {
    return false;
  }

  auto it(scores_.find(score));
  assert(it != scores_.end());

  auto num = it->second.erase(member);
  assert(num == 1);

  return true;
}

PSortedSet::Member2Score::value_type PSortedSet::GetMemberByRank(size_t rank) const {
  if (rank >= members_.size()) {
    rank = members_.size() - 1;
  }

  double score = 0;
  size_t iterRank = 0;

  for (auto it(scores_.begin()); it != scores_.end(); iterRank += it->second.size(), ++it) {
    if (iterRank + it->second.size() > rank) {
      assert(iterRank <= rank);

      score = it->first;
      auto itMem(it->second.begin());
      for (; iterRank != rank; ++iterRank, ++itMem) {
      }

      DEBUG("Get rank {}, name {}", rank, itMem->c_str());
      return std::make_pair(*itMem, score);
    }
  }

  return std::make_pair(PString(), score);
}

size_t PSortedSet::Size() const { return members_.size(); }

std::vector<PSortedSet::Member2Score::value_type> PSortedSet::RangeByRank(long start, long end) const {
  AdjustIndex(start, end, Size());
  if (start > end) {
    return std::vector<Member2Score::value_type>();
  }

  std::vector<Member2Score::value_type> res;
  for (long rank = start; rank <= end; ++rank) {
    res.push_back(GetMemberByRank(rank));
  }

  return res;
}

std::vector<PSortedSet::Member2Score::value_type> PSortedSet::RangeByScore(double minScore, double maxScore) {
  if (minScore > maxScore) {
    return std::vector<Member2Score::value_type>();
  }

  auto itMin = scores_.lower_bound(minScore);
  if (itMin == scores_.end()) {
    return std::vector<Member2Score::value_type>();
  }

  std::vector<Member2Score::value_type> res;
  auto itMax = scores_.upper_bound(maxScore);
  for (; itMin != itMax; ++itMin) {
    for (const auto& e : itMin->second) {
      res.push_back(std::make_pair(e, itMin->first));
    }
  }
  return res;
}

PObject PObject::CreateZSet() {
  PObject obj(PType_sortedSet);
  obj.Reset(new PSortedSet);
  return obj;
}

// commands
#define GET_SORTEDSET(name)                                         \
  PObject* value;                                                   \
  PError err = PSTORE.GetValueByType(name, value, PType_sortedSet); \
  if (err != PError_ok) {                                           \
    ReplyError(err, reply);                                         \
    return err;                                                     \
  }

#define GET_OR_SET_SORTEDSET(name)                                  \
  PObject* value;                                                   \
  PError err = PSTORE.GetValueByType(name, value, PType_sortedSet); \
  if (err != PError_ok && err != PError_notExist) {                 \
    ReplyError(err, reply);                                         \
    return err;                                                     \
  }                                                                 \
  if (err == PError_notExist) {                                     \
    value = PSTORE.SetValue(name, PObject::CreateZSet());           \
  }

PError zadd(const std::vector<PString>& params, UnboundedBuffer* reply) {
  if (params.size() % 2 != 0) {
    ReplyError(PError_syntax, reply);
    return PError_syntax;
  }

  GET_OR_SET_SORTEDSET(params[1]);

  size_t newMembers = 0;
  auto zset = value->CastSortedSet();
  for (size_t i = 2; i < params.size(); i += 2) {
    double score = 0;
    if (!Strtod(params[i].c_str(), params[i].size(), &score)) {
      ReplyError(PError_nan, reply);
      return PError_nan;
    }

    auto it = zset->FindMember(params[i + 1]);
    if (it == zset->end()) {
      zset->AddMember(params[i + 1], score);
      ++newMembers;
    }
  }

  FormatInt(newMembers, reply);
  return PError_ok;
}

PError zcard(const std::vector<PString>& params, UnboundedBuffer* reply) {
  GET_SORTEDSET(params[1]);

  auto zset = value->CastSortedSet();

  FormatInt(static_cast<long>(zset->Size()), reply);
  return PError_ok;
}

PError zrank(const std::vector<PString>& params, UnboundedBuffer* reply) {
  GET_SORTEDSET(params[1]);

  auto zset = value->CastSortedSet();

  int rank = zset->Rank(params[2]);
  if (rank != -1) {
    FormatInt(rank, reply);
  } else {
    FormatNull(reply);
  }

  return PError_ok;
}

PError zrevrank(const std::vector<PString>& params, UnboundedBuffer* reply) {
  GET_SORTEDSET(params[1]);

  auto zset = value->CastSortedSet();

  int rrank = zset->RevRank(params[2]);
  if (rrank != -1) {
    FormatInt(rrank, reply);
  } else {
    FormatNull(reply);
  }

  return PError_ok;
}

PError zrem(const std::vector<PString>& params, UnboundedBuffer* reply) {
  GET_SORTEDSET(params[1]);

  auto zset = value->CastSortedSet();
  long cnt = 0;
  for (size_t i = 2; i < params.size(); ++i) {
    if (zset->DelMember(params[i])) {
      ++cnt;
    }
  }

  FormatInt(cnt, reply);
  return PError_ok;
}

PError zincrby(const std::vector<PString>& params, UnboundedBuffer* reply) {
  GET_OR_SET_SORTEDSET(params[1]);

  double delta;
  if (!Strtod(params[2].c_str(), params[2].size(), &delta)) {
    ReplyError(PError_nan, reply);
    return PError_nan;
  }

  double newScore = delta;
  auto zset = value->CastSortedSet();
  auto itMem = zset->FindMember(params[3]);
  if (itMem == zset->end()) {
    zset->AddMember(params[3], delta);
  } else {
    newScore = zset->UpdateMember(itMem, delta);
  }

  FormatInt(newScore, reply);
  return PError_ok;
}

PError zscore(const std::vector<PString>& params, UnboundedBuffer* reply) {
  GET_SORTEDSET(params[1]);

  auto zset = value->CastSortedSet();
  auto itMem = zset->FindMember(params[2]);
  if (itMem == zset->end()) {
    FormatNull(reply);
  } else {
    FormatInt(itMem->second, reply);
  }

  return PError_ok;
}

static PError GenericRange(const std::vector<PString>& params, UnboundedBuffer* reply, bool reverse) {
  GET_SORTEDSET(params[1]);

  bool withScore = false;
  if (params.size() == 5 && strncasecmp(params[4].c_str(), "withscores", 10) == 0) {
    withScore = true;
  } else if (params.size() >= 5) {
    ReplyError(PError_syntax, reply);
    return PError_syntax;
  }

  long start, end;
  if (!Strtol(params[2].c_str(), params[2].size(), &start) || !Strtol(params[3].c_str(), params[3].size(), &end)) {
    ReplyError(PError_param, reply);
    return PError_param;
  }

  auto zset = value->CastSortedSet();

  auto res(zset->RangeByRank(start, end));
  if (res.empty()) {
    FormatNullArray(reply);
    return PError_ok;
  }

  long nBulk = withScore ? res.size() * 2 : res.size();
  PreFormatMultiBulk(nBulk, reply);

  if (!reverse) {
    for (const auto& s : res) {
      FormatBulk(s.first, reply);
      if (withScore) {
        char score[64];
        int len = Double2Str(score, sizeof score, s.second);

        FormatBulk(score, len, reply);
      }
    }
  } else {
    for (auto it(res.rbegin()); it != res.rend(); ++it) {
      const auto& s = *it;
      FormatBulk(s.first, reply);
      if (withScore) {
        char score[64];
        int len = Double2Str(score, sizeof score, s.second);

        FormatBulk(score, len, reply);
      }
    }
  }

  return PError_ok;
}

// zrange key start stop [WITHSCORES]
PError zrange(const std::vector<PString>& params, UnboundedBuffer* reply) { return GenericRange(params, reply, false); }

// zrange key start stop [WITHSCORES]
PError zrevrange(const std::vector<PString>& params, UnboundedBuffer* reply) {
  return GenericRange(params, reply, true);
}

static PError GenericScoreRange(const std::vector<PString>& params, UnboundedBuffer* reply, bool reverse) {
  GET_SORTEDSET(params[1]);

  bool withScore = false;
  if (params.size() == 5 && strncasecmp(params[4].c_str(), "withscores", 10) == 0) {
    withScore = true;
  } else if (params.size() >= 5) {
    ReplyError(PError_syntax, reply);
    return PError_syntax;
  }

  long minScore, maxScore;
  if (!Strtol(params[2].c_str(), params[2].size(), &minScore) ||
      !Strtol(params[3].c_str(), params[3].size(), &maxScore)) {
    ReplyError(PError_nan, reply);
    return PError_nan;
  }

  auto zset = value->CastSortedSet();

  auto res(zset->RangeByScore(minScore, maxScore));
  if (res.empty()) {
    FormatNull(reply);
    return PError_ok;
  }

  long nBulk = withScore ? res.size() * 2 : res.size();
  PreFormatMultiBulk(nBulk, reply);

  if (!reverse) {
    for (const auto& s : res) {
      FormatBulk(s.first, reply);
      if (withScore) {
        char score[64];
        int len = Double2Str(score, sizeof score, s.second);

        FormatBulk(score, len, reply);
      }
    }
  } else {
    for (auto it(res.rbegin()); it != res.rend(); ++it) {
      const auto& s = *it;
      FormatBulk(s.first, reply);
      if (withScore) {
        char score[64];
        int len = Double2Str(score, sizeof score, s.second);

        FormatBulk(score, len, reply);
      }
    }
  }

  return PError_ok;
}

PError zrangebyscore(const std::vector<PString>& params, UnboundedBuffer* reply) {
  return GenericScoreRange(params, reply, false);
}

PError zrevrangebyscore(const std::vector<PString>& params, UnboundedBuffer* reply) {
  return GenericScoreRange(params, reply, true);
}

static PError GenericRemRange(const std::vector<PString>& params, UnboundedBuffer* reply, bool useRank) {
  GET_SORTEDSET(params[1]);

  double start, end;
  if (!Strtod(params[2].c_str(), params[2].size(), &start) || !Strtod(params[3].c_str(), params[3].size(), &end)) {
    ReplyError(PError_nan, reply);
    return PError_nan;
  }

  std::vector<PSortedSet::Member2Score::value_type> res;
  auto zset = value->CastSortedSet();
  if (useRank) {
    long lstart = static_cast<long>(start);
    long lend = static_cast<long>(end);
    AdjustIndex(lstart, lend, zset->Size());
    res = zset->RangeByRank(lstart, lend);
  } else {
    res = zset->RangeByScore(start, end);
  }

  if (res.empty()) {
    Format0(reply);
    return PError_ok;
  }

  for (const auto& s : res) {
    bool succ = zset->DelMember(s.first);
    assert(succ);
  }

  if (zset->Size() == 0) {
    PSTORE.DeleteKey(params[1]);
  }

  FormatInt(static_cast<long>(res.size()), reply);
  return PError_ok;
}

PError zremrangebyrank(const std::vector<PString>& params, UnboundedBuffer* reply) {
  return GenericRemRange(params, reply, true);
}

PError zremrangebyscore(const std::vector<PString>& params, UnboundedBuffer* reply) {
  return GenericRemRange(params, reply, false);
}

}  // namespace pikiwidb
